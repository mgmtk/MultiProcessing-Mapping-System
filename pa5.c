/* pa5.c */
//Mitchell Mesecher


#include <stdio.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <mqueue.h>

#include "common.h"
#include "classify.h"
#include "intqueue.h"

//constant priority for all tasks
const int PRIORITY = 1;

void term( mqd_t tasks);

int main(int argc, char *argv[])
{
    int input_fd, classification_fd, map_fd;
    pid_t pid;
    off_t file_size;
    mqd_t tasks_mqd, results_mqd; // message queue descriptors
    struct mq_attr attr;    // struct used to configure a message queue
    char tasks_mq_name[16]; // name of the tasks queue
    char results_mq_name[18];   // name of the results queue
    int num_clusters;

    if (argc != 2) {
        printf("Usage: %s data_file\n", argv[0]);
        return 1;
    }

    //open input file for reading
    input_fd = open(argv[1], O_RDONLY);
    if (input_fd < 0) {
        printf("Error opening file \"%s\" for reading: %s\n", argv[1], strerror(errno));
        return 1;
    } 

    //open classification file for reading and writing
    classification_fd = open(CLASSIFICATION_FILE, O_RDWR | O_CREAT | O_TRUNC, 0666);
    if (classification_fd < 0) {
        printf("Error creating file \"%s\": %s\n", CLASSIFICATION_FILE, strerror(errno));
        return 1;
    }
    
    //open map file for reading 
    map_fd = open(MAP_FILE, O_WRONLY | O_CREAT | O_TRUNC, 0600);
    if (map_fd < 0) {
        printf("Error creating file \"%s\": %s\n", MAP_FILE, strerror(errno));
        return 1;
    }

    // Determine the file size of the input file
    file_size = lseek(input_fd, 0, SEEK_END);
    lseek(input_fd, 0, SEEK_SET);
    
    // Calculate how many clusters are present
    num_clusters = file_size / CLUSTER_SIZE;
 
    // Generate the names for the tasks and results queue
    snprintf(tasks_mq_name, 16, "/tasks_%s", getlogin());
    tasks_mq_name[15] = '\0';
    snprintf(results_mq_name, 18, "/results_%s", getlogin());
    results_mq_name[17] = '\0';

    //set necassary attributes for the message queues.
    //Other two fields are ignored by mq_open
    attr.mq_maxmsg = 1000;
    attr.mq_msgsize = MESSAGE_SIZE_MAX;
  
    // Create the child processes
    for (int i = 0; i < NUM_PROCESSES; i++) {
        pid = fork();
        if (pid == -1)
            exit(1);
        else if (pid == 0) {

            // All the worker code must go here
            struct result res; //result struct
            struct task *my_task; //task struct
            
            char cluster_data[CLUSTER_SIZE]; //cluster buffer
            char recv_buffer[MESSAGE_SIZE_MAX]; //recieve buffer
            int offset; //offset into file
            int a_jpg; //jpg flag
            unsigned char classification; //class byte
			unsigned int task_prio; //priority fill
            unsigned int file; //continous file num
            unsigned int cluster; //cluster map offset 

            // Here you need to create/open the two message queues for the
            // worker process
            tasks_mqd = mq_open(tasks_mq_name, O_RDWR | O_CREAT, 0600, &attr); //open tasks queue with specified attributes and flags 
			if(tasks_mqd < 0)
			{
			    //check not make sure the queue was opened correctly
                printf("Error opening tasks for reading c : %s\n" , strerror(errno));
				return 1;
			}
            results_mqd = mq_open(results_mq_name, O_RDWR | O_CREAT, 0600, &attr); //open results queue with specified attributes and flags
            if(results_mqd < 0)
			{
			    //check not make sure the queue was opened correctly
                printf("Error opening results for reading c: %s\n", strerror(errno));
				return 1;
			}


            // A worker process must endlessly receive new tasks
            // until instructed to terminate
            while(1) {

                // receive the next task message here
                mq_receive(tasks_mqd, recv_buffer, MESSAGE_SIZE_MAX, &task_prio);

                // cast the received message to a struct task
                my_task = (struct task *)recv_buffer;
               
                switch (my_task->task_type) {
                    case TASK_CLASSIFY:
                        // you must retrieve the data for the specified cluster
                        // and stpre it in cluster_data before executing the
                        // code below
                        
                        //determine the offset into the input file
                        offset = (my_task->task_cluster * CLUSTER_SIZE); 
                        
                        //read bytes of size CLUSTER_SIZE from the specified offset
                        pread(input_fd, cluster_data, CLUSTER_SIZE, offset);
                        
                        // Classification code
                        classification = TYPE_UNCLASSIFIED;
                        if (is_jpg(cluster_data))
                            classification |= TYPE_IS_JPG;
                        if (has_jpg_header(cluster_data))
                            classification |= TYPE_JPG_HEADER | TYPE_IS_JPG;
                        if (has_jpg_footer(cluster_data))
                            classification |= TYPE_JPG_FOOTER | TYPE_IS_JPG;
                        if (classification == TYPE_UNCLASSIFIED) {
                            if (is_html(cluster_data))
                                classification |= TYPE_IS_HTML;
                            if (has_html_header(cluster_data))
                                classification |= TYPE_HTML_HEADER | TYPE_IS_HTML;
                            if (has_html_footer(cluster_data))
                                classification |= TYPE_HTML_FOOTER | TYPE_IS_HTML;
                        }
                        if (classification == TYPE_UNCLASSIFIED)
                            classification = TYPE_UNKNOWN;

                        // prepare a results message and send it to results
                        // queue here
                        res.res_cluster_number = my_task->task_cluster; //assign cluster number to result record
                        res.res_cluster_type = classification; //assign classification byte to result record 
                      
                        //send the results record to the result message queue for supervisor handling
                        mq_send(results_mqd, (char*) &res, sizeof(res), PRIORITY); 
                        break;
                        
                    case TASK_MAP: 
                    //CHANGED SIZE OF FILE NAME
                        // implement the map task logic here

                        //determine the header cluster number
                        cluster = my_task->task_cluster;
                        file = 0;
                        a_jpg = 0;
                        
                        //determine if header is jpg or html
                        if(pread(classification_fd, &classification, sizeof(char), cluster) 
                        && (classification & TYPE_IS_JPG))
                        {
                            a_jpg = 1;
                        }
                        
                        //write the header filename string to the map file at the cluster number offset 
                        pwrite(map_fd, my_task->task_filename, sizeof(my_task->task_filename), cluster * 16);
                        
                        //write the file number to the map file at the cluster number offset + 12 
                        //to account for the filename string alreadt written
                        pwrite(map_fd, &file, sizeof(int), (cluster * 16) + 12);
                        
                        //check that one cluster is both footer and header
                        if((classification & TYPE_JPG_FOOTER) || (classification & TYPE_HTML_FOOTER))
                        {
                            break;
                        }
                        //increment the cluster and file nums
                        cluster++;
                        file++;
                        
                        if(a_jpg)//check if header was jpg
                        {
                            
                            //continue reading while there are bytes to be read from the class file 
                            while(pread(classification_fd, &classification, sizeof(char), cluster))
                            {
                                //make sure the byte read is of type jpg and skip all html clusters in between
                                if(classification & TYPE_IS_JPG)
                                {
                                    //write the same file name to the new offset but update the cluster being written 
                                    pwrite(map_fd, my_task->task_filename, sizeof(my_task->task_filename), cluster * 16);
                                    
                                    //write the cluster number 
                                    pwrite(map_fd, &file, sizeof(int), (cluster * 16) + 12);
                                    file++;
                                }
                                //if a jpg footer has been found then break the loop as all clusters in this file have been written
                                if(classification & TYPE_JPG_FOOTER)
                                {
                                    break;
                                }
                                cluster++; //increment the file num
                            }
                        }
                        else//if header wasnt jpg then write html
                        {
                            //continue reading while there are bytes to be read from the class file                            
                            while(pread(classification_fd, &classification, sizeof(char), cluster))
                            {
                                //make sure the byte read is of type html and skip all jpg clusters in between                              
                                if(classification & TYPE_IS_HTML)
                                {
                                    //write the same file name to the new offset but update the cluster being written 
                                    pwrite(map_fd, my_task->task_filename, sizeof(my_task->task_filename), cluster * 16);
                                    
                                    //write the cluster number 
                                    pwrite(map_fd, &file, sizeof(int), (cluster * 16) + 12);
                                    file++;
                                }
                                //if a html footer has been found then break the loop as all clusters in this file have been written                                
                                if(classification & TYPE_HTML_FOOTER)
                                {
                                    break;
                                }
                                cluster++; //increment the file num
                            }
                        }
                        break;
                        
                    default:
                        // implement the terminate task logic here 
                        //close worker message queue descriptors 
                        close(map_fd);
                        close(input_fd);
                        close(classification_fd);                    
                        mq_close(tasks_mqd);
                        mq_close(results_mqd);
                        exit(0);
                }
            }
        }
    }

    // All the supervisor code needs to go here
    
    struct intqueue headerq;

    // Initialize an empty queue to store the clusters that have file headers.
    // This queue needs to be populated in Phase 1 and worked off in Phase 2.
    initqueue(&headerq);

    
   
    //delare necassary variables
    int classified_tasks = 0; //sent classified tasks
    int written_tasks = 0; //number of written result tasks
    int err = 0; //error flag
    unsigned int result_priority; //priority of recieved message
    
    //create the attribute and task structs
    struct mq_attr new_attr; //attr to change queue to blocking
    struct task new_task; //task to send
    struct result my_task; //result ro recieve

    //open tasks queue descriptor with specified attributes and flags
    tasks_mqd = mq_open(tasks_mq_name, O_RDWR | O_CREAT | O_NONBLOCK, 0600, &attr);
    if(tasks_mqd < 0)
    {
        //check for failure to open mqd
        printf("Error opening tasks for reading: p %s\n", strerror(errno));
        return 1;
    }
    
    //open results queue descriptor with specified attributes and flags
    results_mqd = mq_open(results_mq_name, O_RDWR | O_CREAT, 0600, &attr);
    if( results_mqd < 0)
    {
        //check for failure to open mqd
        printf("Error opening file results for reading: p%s\n", strerror(errno));
        return 1;
    }


    //Implement Phase 1 here
    
    //continue to iterate while all clasifiy tasks have not been sent
    while(classified_tasks < num_clusters) 
    {
        
        new_task.task_cluster = classified_tasks; //assign cluster number to task record
        new_task.task_type = TASK_CLASSIFY; //specify a classify task
        
        //try to send task record to the tasks message queue
        err = mq_send(tasks_mqd, (char *) &new_task, sizeof(new_task), PRIORITY);
        if(err >= 0) //check send success
        {
            //increment the number of tasks sent on send success
            classified_tasks++;
        }
        else if(err == -1 && errno == EAGAIN) //check if MQ is full and send failed due to EAGAIN error
        {
             
            //Give workers time to process tasks by recieving and writing a result record
            mq_receive(results_mqd, (char*) &my_task, MESSAGE_SIZE_MAX, &result_priority); // recieve a result record struct
            
            //check if the cluster recieved was a jpg or html header
            if((my_task.res_cluster_type & TYPE_JPG_HEADER) || (my_task.res_cluster_type & TYPE_HTML_HEADER))
            {
                //if was header add cluster number to header queue 
                enqueue(&headerq, my_task.res_cluster_number);
            }
            //write classification byte to class file at the correct offset
            pwrite(classification_fd, &my_task.res_cluster_type, sizeof(my_task.res_cluster_type), my_task.res_cluster_number);
            written_tasks++; //increment written tasks
            err = 0; //set err flag back to 0

        }
        else //if error was not set to EAGAIN due to full queue
        {
            printf("%s\n", strerror(errno));
            term(tasks_mqd); //terminate the child processes 
            exit(0); //exit program
        
        }

    }
    //write all the remaining result records 
    while(written_tasks < num_clusters)
    {
            //recieve result record from results queue
        mq_receive(results_mqd, (char*) &my_task, MESSAGE_SIZE_MAX, &result_priority);
          
        //write classification byte to class file at the correct offset            
        if((my_task.res_cluster_type & TYPE_JPG_HEADER) || (my_task.res_cluster_type & TYPE_HTML_HEADER)) 
        {
            //if was header add cluster number to header queue             
            enqueue(&headerq, my_task.res_cluster_number);
        }
        //write classification byte to class file at the correct offset
        pwrite(classification_fd, &my_task.res_cluster_type, sizeof(my_task.res_cluster_type), my_task.res_cluster_number);
        written_tasks++; //incrememnt written tasks 
    
    }
    
    // Implement Phase 2 here
    
    //set flags to blocking
    new_attr.mq_flags = 0;
    
    //set attr only modifies blocking flag
    mq_setattr(tasks_mqd, &new_attr, &attr);
   
    unsigned char classification; //class byte
    int file_num = 1; //file number
    
    //continue iterating until queue is empty
    while(!isempty(&headerq)) 
    {
        //set task type to map
        new_task.task_type = TASK_MAP;
        //retrieve the cluster number of the headers from queue
        new_task.task_cluster = dequeue(&headerq);
        
        //read the classification byte from the class file
        pread(classification_fd, &classification, sizeof(char), new_task.task_cluster); 
        
        //check if jpg
        if(classification & TYPE_JPG_HEADER)
        { 
            //build filename jpg string and write it to task record
            sprintf(new_task.task_filename, "file%04d.%s", file_num, "jpg");
            file_num++;// increment file num
            
        }
        else if(classification & TYPE_HTML_HEADER) //check if html
        {
            //build filename html string and write it to task record
             sprintf(new_task.task_filename, "file%04d.%s", file_num, "htm");
             file_num++;
            
        }
        //send map task to task queue 
        err = mq_send(tasks_mqd, (char *) &new_task, sizeof(new_task), PRIORITY);
        if(err < 0)
        {
            //check error flag to see if send failed
            printf("%s\n", strerror(errno));
            term(tasks_mqd);
            exit(0);
            
        }
       
    
    }
   
    // Implement Phase 3 here
    //close all file descriptors and queue refrences
    term(tasks_mqd);
    mq_close(tasks_mqd);
    mq_close(results_mqd);
    mq_unlink(tasks_mq_name);
    mq_unlink(results_mq_name);
    close(map_fd);
    close(input_fd);
    close(classification_fd);
    return 0;
};

//Function to kill all worker processes
//Sends term tasks for the number of processes created
void term(mqd_t tasks)
{
    struct task new_task;
    
    for(int i = 0; i < NUM_PROCESSES; i++)
    {
        new_task.task_type = TASK_TERMINATE; //specify class terminate
        mq_send(tasks, (char *) &new_task, sizeof(new_task), PRIORITY); //send terminate task
        wait(NULL);

    }

}


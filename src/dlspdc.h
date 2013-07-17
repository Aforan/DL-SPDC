#ifndef DLSPDC_H
#define DLSPDC_H

#include <list>
#include "mpi.h"
#include "hdfs.h"
#include <string>
#include <vector>
#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <unistd.h>
#include <sys/time.h>
#include <sys/stat.h>

#define DEFAULT_FILE_SYSTEM "default"

#define REGISTER_JOB 				0
#define REGISTER_JOB_SUCCESS 		1
#define REGISTER_JOB_FAILURE 		2
#define REGISTER_FILENAME			3
#define JOB_REGISTRATION_FINISHED	4
#define JOB_FIN_REGISTRATION_CONF	5
#define MD_LOCALITY_DISTRIBUTION	6
#define MD_LOCALITY_DIST_HOSTNAME	7
#define MD_LOCALITY_DIST_CHUNKS		8
#define MD_LOCALITY_DIST_DONE		9
#define MD_LOCALITY_DIST_MORE		10
#define SLAVE_CHECK_IN_BEGIN		11
#define SLAVE_CHECK_IN				12
#define SLAVE_CHECK_IN_RESP			13
#define MD_SLAVE_ASS_DIST			14
#define MD_SLAVE_ASS_DIST_HOSTNAME	15
#define MD_SLAVE_ASS_DIST_MORE		16
#define MD_SLAVE_ASS_DIST_DONE		17
#define SLAVE_INIT_NUM_CHUNKS		18
#define SLAVE_INIT_LOC_CHUNKS		19
#define SLAVE_INIT_CHUNKS_NONE		20
#define DEBUG_MESSAGE_INIT			21
#define DEBUG_MESSAGE				22
#define DEBUG_SEQUENCE_INIT			23
#define DEBUG_SEQ_MSG_LEN			24
#define DEBUG_SEQ_MSG 				25
#define DEBUG_SEQ_FINALIZE			26
#define DEBUG_KILL_SERVER			27
#define BEGIN_SLAVE_JOB_DIST		28
#define SLAVE_JOB_DIST_FILENAME		29
#define SLAVE_JOB_DIST_CHUNKS		30
#define END_SLAVE_JOB_DIST			31
#define IS_JOB_AVAILABLE			32
#define JOB_REQUEST_RESPONSE		33
#define JOB_REQUEST_RESOLUTION		34
#define JOB_REQUEST_RESOLUTION_FINISHED 35
#define MD_SLAVE_ASS_DIST_RANKS		36
#define SLAVE_FINALIZATION			37
#define DEBUG_FINALIZATION			38
#define MD_FINALIZATION				39
#define SLAVE_SEND_UPDATED_JOBS		40
#define SLAVE_JOB_UPDATE 			41

#define REGISTERED_STATUS			0
#define SCHEDULED_STATUS			1
#define COMPLETED_STATUS			3

#define MAX_FILENAME_SIZE			512
#define MAX_HOSTNAME_SIZE			256

#define UN_ALLOCATED				0
#define ALLOCATED 					1

typedef struct SPDC_Settings_Struct {
	int nthreads;
	int* md_ranks;
	int* slave_ranks;
	int num_slaves;
	int num_md_servers;
	int master_rank;
	int debug_rank;
	int debug_mode;
	MPI_Comm comm_group;
} SPDC_Settings;

typedef struct SPDC_HDFS_Job_Struct {
	int status;
	int id;
	int tag;
	char* filename;
	int filename_length;
	uint64_t start_offset;
	uint64_t length;
	int* included_chunks;
	int num_included_chunks;
} SPDC_HDFS_Job;

typedef struct SPDC_HDFS_File_Info_Struct {
	char* filename;
	uint64_t size;
	uint64_t chunk_size;
	int replication_factor;
} SPDC_HDFS_File_Info;

typedef struct SPDC_HDFS_Host_Chunk_Map_Struct {
	char* hostname;
	int* chunks;
	int num_chunks;
	int* ranks;
	int num_ranks;
} SPDC_HDFS_Host_Chunk_Map;

typedef struct SPDC_Hostname_Rank_Struct {
	char* hostname;
	int* ranks;
	int num_ranks;
} SPDC_Hostname_Rank;

typedef struct SPDC_HDFS_Slave_Info_Struct{
	int num_chunks;
	int* chunks;
} SPDC_HDFS_Slave_Info;

typedef struct SPDC_HDFS_Job_Request_Struct{
	int requester;
	int job_id;
	int status;
	int valid;
} SPDC_HDFS_Job_Request;

int SPDC_Init(SPDC_Settings* set, int caller_rank);
int SPDC_Register_HDFS_Job(SPDC_HDFS_Job* job);
int SPDC_Master_Init();
int SPDC_Init_Slave();
int SPDC_Init_MD_Server();
int SPDC_Finalize_Registration();
int SPDC_Build_Initial_Jobs();
int SPDC_MD_Server();

void SPDC_Debug_Print_Jobs();
void SPDC_Build_Locality_Map();
void SPDC_Distribute_Locality_Map();
void SPDC_Receive_Locality_Map();
void SPDC_Receive_Slave_Checkins();
void SPDC_Receive_Slave_Checkin_Dist();
void SPDC_Distribute_Slave_Checkins();
void SPDC_Build_Chunk_Job_Map();
void SPDC_Distribute_Slave_Jobs();
void SPDC_Slave_Receive_Init_Jobs();
void SPDC_Debug_Server();
void SPDC_Debug_Server_Init();
void SPDC_Debug_Message(char* msg);
void SPDC_Kill_Debug_Server();
void SPDC_Send_Debug_Sequence_Message(char* msg);
void SPDC_End_Debug_Sequence();
void SPDC_Begin_Debug_Sequence();
void SPDC_Slave_Init_Chunks();
void SPDC_Send_Slave_Job(SPDC_HDFS_Job* job, int slave_rank);
void SPDC_Distribute_Slave_Chunks();
void SPDC_Slave_Sort_Jobs();
void SPDC_Resolve_Request(SPDC_HDFS_Job_Request recv_reququest);
void SPDC_Receive_Request_Resolution();
void SPDC_Send_Request_Resolutions();
void SPDC_Send_Request_Response();
void SPDC_Receive_Job_Requests();
void SPDC_Finalize_Slave();
int SPDC_Check_Finished();
void SPDC_MD_Finalize();
void SPDC_Update_Jobs();
void SPDC_Send_Job_Update();

bool SPDC_Compare_Job(const SPDC_HDFS_Job *a, const SPDC_HDFS_Job *b);
SPDC_HDFS_Job* SPDC_Get_Next_Job(int depth);

#endif

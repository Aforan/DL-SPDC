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

#define REGISTERED_STATUS			0
#define SCHEDULED_STATUS			1
#define COMPLETED_STATUS			3

#define MAX_FILENAME_SIZE			512
#define MAX_HOSTNAME_SIZE			256

typedef struct SPDC_Settings_Struct {
	int nthreads;
	int* md_ranks;
	int* slave_ranks;
	int num_slaves;
	int num_md_servers;
	int master_rank;
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
	int rank;
} SPDC_HDFS_Host_Chunk_Map;

typedef struct SPDC_Hostname_Rank_Struct {
	char* hostname;
	int rank;
} SPDC_Hostname_Rank;

typedef struct SPDC_HDFS_Slave_Info_Struct{
	int num_chunks;
	int* chunks;
} SPDC_HDFS_Slave_Info;

int SPDC_Init(SPDC_Settings* set, int caller_rank, int debug_mode);
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
#endif

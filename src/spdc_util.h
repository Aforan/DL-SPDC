#ifndef SPDC_UTIL_H
#define SPDC_UTIL_H

#include "dlspdc.h"
#include <vector>

using namespace std;

#define UNKNOWN_ERR				-1
#define SETTINGS_NULL_ERR 		 0
#define INVALID_SETTINGS_ERR 	 1

void SPDC_Abort(int err_code);
void SPDC_Aborts(int err_code, char* err_msg);
void print_settings(SPDC_Settings* set);
void print_locality_map(vector<SPDC_HDFS_Host_Chunk_Map*>* vec);
void print_job(SPDC_HDFS_Job* job);
void print_jobs(vector<SPDC_HDFS_Job*> *vec);
void print_locality_map_elem(SPDC_HDFS_Host_Chunk_Map* e);
void print_slave_hostname(vector<SPDC_Hostname_Rank*> *vec);
void print_slave_info(SPDC_HDFS_Slave_Info* slave);
int validate_settings(SPDC_Settings* set);
int contains_file_info(char* filename, vector<SPDC_HDFS_File_Info*> *vec);
int contains_host(char* hostname, vector<SPDC_HDFS_Host_Chunk_Map*> *vec);
int get_ranks_from_hostname(char* hostname, vector<SPDC_Hostname_Rank*> *vec, int* buf);
char* get_hostname_from_rank(int rank, vector<SPDC_Hostname_Rank*> *vec);
SPDC_HDFS_File_Info* get_file_info(char* filename, vector<SPDC_HDFS_File_Info*> *vec);
SPDC_HDFS_Host_Chunk_Map* get_chunk_map(char* hostname, vector<SPDC_HDFS_Host_Chunk_Map*> *vec);
SPDC_HDFS_Host_Chunk_Map* get_chunk_map_from_rank(int rank, vector<SPDC_HDFS_Host_Chunk_Map*> *vec);
SPDC_Hostname_Rank* find_hnr_from_hostname(char* hostname, vector<SPDC_Hostname_Rank*> *vec);
#endif

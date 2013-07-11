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
void print_locality_map_elem(SPDC_HDFS_Host_Chunk_Map* e);
int validate_settings(SPDC_Settings* set);
int contains_file_info(char* filename, vector<SPDC_HDFS_File_Info*> *vec);
int contains_host(char* hostname, vector<SPDC_HDFS_Host_Chunk_Map*> *vec);

SPDC_HDFS_File_Info* get_file_info(char* filename, vector<SPDC_HDFS_File_Info*> *vec);
SPDC_HDFS_Host_Chunk_Map* get_chunk_map(char* hostname, vector<SPDC_HDFS_Host_Chunk_Map*> *vec);

#endif

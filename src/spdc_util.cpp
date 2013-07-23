#ifndef SPDC_UTIL_CPP
#define SPDC_UTIL_CPP

#include "spdc_util.h"

void SPDC_Abort(int err_code) {
	fprintf(stderr, "Got err code: %d\n", err_code);

}

void SPDC_Aborts(int err_code, char* err_msg) {
	fprintf(stderr, "Got err code: %d with error message \"%s\"", err_code, err_msg);
}

int validate_settings(SPDC_Settings* set) {
	//print_settings(set);
	return 0;
}

void print_settings(SPDC_Settings* set) {
	fprintf(stdout, "Settings Struct:\n");
	fprintf(stdout,	   	"\tnthreads: %d\n"
						"\tnum_md_servers: %d\n"
						"\tnum_slaves: %d\n"
						"\tmaster_rank: %d\n",
						set->nthreads,
						set->num_md_servers,
						set->num_slaves,
						set->master_rank);

	fprintf(stdout, "\tmd_ranks:");

	for(int i = 0; i < set->num_md_servers; i++) {
		fprintf(stdout, " %d,", set->md_ranks[i]);
	}

	fprintf(stdout, "\n");

	fprintf(stdout, "\tslaves:");

	for(int i = 0; i < set->num_slaves; i++) {
		fprintf(stdout, " %d,", set->slave_ranks[i]);
	}

	fprintf(stdout, "\n");
}

void print_job(SPDC_HDFS_Job* job) {
	fprintf(stdout, "Job # %d:\n", job->id);
	fprintf(stdout,	   	"\tstatus: %d\n"
						"\ttag: %d\n"
						"\tfilename: %s\n"
						"\tstart: %lu\n"
						"\tlength: %lu\n",
						job->status,
						job->tag,
						job->filename,
						job->start_offset,
						job->length);

	fprintf(stdout, "\tchunks:");

	for(int i = 0; i < job->num_included_chunks; i++) {
		fprintf(stdout, " %d,", job->included_chunks[i]);
	}

	fprintf(stdout, "\n");
}


int contains_file_info(char* filename, vector<SPDC_HDFS_File_Info*> *vec) {
	for(uint i = 0; i < vec->size(); i++) {
		if(!strcmp(filename, vec->at(i)->filename)) return 1;
	}

	return 0;
}

int contains_host(char* hostname, vector<SPDC_HDFS_Host_Chunk_Map*> *vec) {
	for(uint i = 0; i < vec->size(); i++) {
		if(!strcmp(hostname, vec->at(i)->hostname)) return 1;
	}

	return 0;
}

SPDC_HDFS_File_Info* get_file_info(char* filename, vector<SPDC_HDFS_File_Info*> *vec) {
	for(uint i = 0; i < vec->size(); i++) {
		if(!strcmp(filename, vec->at(i)->filename)) return vec->at(i);
	}

	return NULL;
}

SPDC_HDFS_Host_Chunk_Map* get_chunk_map(char* hostname, vector<SPDC_HDFS_Host_Chunk_Map*> *vec) {
	for(uint i = 0; i < vec->size(); i++) {
		if(!strcmp(hostname, vec->at(i)->hostname)) return vec->at(i);
	}

	return NULL;
}

void print_locality_map(vector<SPDC_HDFS_Host_Chunk_Map*>* vec) {
	for(uint i = 0; i < vec->size(); i++) {
		print_locality_map_elem(vec->at(i));
	}
}

void print_locality_map_elem(SPDC_HDFS_Host_Chunk_Map* e) {
	fprintf(stdout, "host: %s\n\tchunks: ", e->hostname);

	for(int i = 0; i < e->num_chunks; i++) {
		fprintf(stdout, "%d, ", e->chunks[i]);
	}

	fprintf(stdout, "\n");
}

void print_slave_hostname(vector<SPDC_Hostname_Rank*> *vec) {
	//for(uint i = 0; i < vec->size(); i++) {
	//	fprintf(stdout, "\t%d:\t%s\n", vec->at(i)->rank, vec->at(i)->hostname);
	//}
}

void print_jobs(vector<SPDC_HDFS_Job*> *vec) {
	for(uint i = 0; i < vec->size(); i++) {
		print_job(vec->at(i));
	}
}

void print_slave_info(SPDC_HDFS_Slave_Info* slave) {
	fprintf(stdout, "\t***local chunks: ");

	for(int i = 0; i < slave->num_chunks; i++) {
		fprintf(stdout, " %d", slave->chunks[i]);
	}

	fprintf(stdout, "\n");
}

int get_ranks_from_hostname(char* hostname, vector<SPDC_Hostname_Rank*> *vec, int* buf) {
	for(uint i = 0; i < vec->size(); i++) {
		if(!strcmp(vec->at(i)->hostname, hostname)) {
			buf = vec->at(i)->ranks;
			return vec->at(i)->num_ranks;
		}
	}

	return 0;
}

SPDC_HDFS_Host_Chunk_Map* get_chunk_map_from_rank(int rank, vector<SPDC_HDFS_Host_Chunk_Map*> *vec) {
	if(vec == NULL) return NULL;

	for(uint i = 0; i < vec->size(); i++) {
		for(int j = 0; j < vec->at(i)->num_ranks; j++) {
			if(vec->at(i)->ranks[j] == rank) return vec->at(i);
		}
	}

	return NULL;
}

int num_similar(int* a, int a_len, int* b, int b_len) {
	int r = 0;
	//	Dumb algorithm for now
	for(int i = 0; i < a_len; i++) {
		for(int j = 0; j < b_len; j++) {
			if(a[i] == b[j]) r++;
		}
	}

	return r;
}

SPDC_Hostname_Rank* find_hnr_from_hostname(char* hostname, vector<SPDC_Hostname_Rank*> *vec) {
	for(uint i = 0; i < vec->size(); i++) {
		if(!strcmp(hostname, vec->at(i)->hostname)) return vec->at(i);
	}

	return NULL;
}

char* get_hostname_from_rank(int rank, vector<SPDC_Hostname_Rank*> *vec) {
	for(uint i = 0; i < vec->size(); i++) {
		for(int j = 0; j < vec->at(i)->num_ranks; j++) {
			if(vec->at(i)->ranks[j] == rank) return vec->at(i)->hostname;
		}
	}

	return NULL;
}

#endif

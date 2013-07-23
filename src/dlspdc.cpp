/*
 *	dlspdc.cpp
 *	Author: Andrew Foran
 *	
 *	Contents: All core interface functions.
 *	Conventions: Underscore not camelcase!!!!!!!!!
 *	
 *	TODO:	Need to add multiple files for locality_vector.
 *			Idea:	Have one vec per file with unique mas structs
 *					per file
 *
 *			Should make sure all recv protected with probes.
 *
 *
 *	Should update slaves with job done data
 *
 *
 *	Sim:
 *	Try and get / distribute all slave checkins
 *	Select random vals betwwen range of that vec
 *	Allocate chunks to those slaves / machines
 */

#ifndef DLSPDC_CPP
#define DLSPDC_CPP

#include "dlspdc.h"
#include "spdc_util.cpp"


using namespace std;

vector<SPDC_HDFS_Job*> *job_vector;
vector<SPDC_HDFS_Job*> *done_jobs;
vector<SPDC_HDFS_File_Info*> *file_info_vector;
vector<SPDC_HDFS_Host_Chunk_Map*> *locality_vector;
vector<SPDC_Hostname_Rank*> *slave_hostname_vector;
vector<SPDC_HDFS_Job_Request*> *request_queue;
vector<int> *sorted_md_ranks;
/*
 *	The rank of the process that called init.  We can just
 *	Store it here for later use, simplifying some things.
 */
int our_rank;
int md_id;
int sl_id;
int md_rank;

int sim_flag = 1;

int num_slaves_resp;
int* slaves;
int debug_sequence_initialized;
int* changed_jobs;
int num_changed_jobs;
/*
 *	Debug Flag 0 by Default
 */
 int debug_flag = 0;

 int num_jobs;

/*
 *	Settings used to store number of threads, metadata server ranks
 *	master rank, comm group ... (Anything else we may need here)
 */
SPDC_Settings* settings;

SPDC_HDFS_Slave_Info* slave_info;

FILE* debug_log = NULL;

/*
 *	Everyone calls init AFTER MPI is initialized.  Init then initializes
 *	each process according to the settings, eg: slaves get SPDC_Slave_init etc.
 *	Once all processes have been initialized, control is returned to the user.
 */
int SPDC_Init(SPDC_Settings* set, int caller_rank) {

	//	Check for null settings
	if (set == NULL) {
		SPDC_Abort(SETTINGS_NULL_ERR);
	} else {
		int valid = validate_settings(set);

		//	Check for valid settings
		if(valid == -1) {
			SPDC_Abort(INVALID_SETTINGS_ERR);
		}

		settings = set;
		debug_sequence_initialized = 0;
	}

	int r;
	our_rank = caller_rank;

	if(our_rank == settings->master_rank) {
		//	Initialize master rank
		r = SPDC_Master_Init();
	} else {
		int i, done = 0;

		if(settings->debug_mode && our_rank == settings->debug_rank) {
			SPDC_Debug_Server_Init();
			done = 1;
		}

		//	Check if we are a metadata server
		for(i = 0; i < settings->num_md_servers; i++) {
			if(our_rank == settings->md_ranks[i]) {
				//	Initialize md server

				r = SPDC_Init_MD_Server();
				//SPDC_Kill_Debug_Server();
				done = 1;
				break;
			}
		}

		//	If we werent a md server, check if we are a slave
		if(!done) {
			for(i = 0; i < settings->num_slaves; i++) {
				if(our_rank == settings->slave_ranks[i]) {
					//	Initialize slave
					r = SPDC_Init_Slave();
					done = 1;
					break;
				}
			}
		}

		//	If we still aren't done, there is a problem.  This should not happen
		if(!done) {
			char err_msg[200];
			sprintf(err_msg, "Rank: %d could not be identified", our_rank);
			SPDC_Aborts(UNKNOWN_ERR, err_msg);
		}
	}

	return r;
}

int SPDC_Register_HDFS_Job(SPDC_HDFS_Job* job) {
	MPI_Status status;
	int rc = 0, dummy;

	/*
	 *	Send the job registration to the primary metadata server,
	 *  wait for the response and return accordingly (-1 fail, 0 success)
	 */

	if(job != NULL) {

		for(int i = 0; i < settings->num_md_servers; i++) {
			MPI_Send(	job, 
						sizeof(SPDC_HDFS_Job), 
						MPI_BYTE, 
						settings->md_ranks[i], 
						REGISTER_JOB, 
						settings->comm_group);

			MPI_Recv(	&dummy, 
						1, 
						MPI_INT, 
						settings->md_ranks[i], 
						MPI_ANY_TAG, 
						settings->comm_group, 
						&status);

			rc |= (status.MPI_TAG == REGISTER_JOB_SUCCESS) ? 0 : -1;

			//	Need to send the filename seperately
			MPI_Send(	job->filename, 
						job->filename_length, 
						MPI_CHAR, 
						settings->md_ranks[i], 
						REGISTER_FILENAME, 
						settings->comm_group);

			MPI_Recv(	&dummy, 
						1, 
						MPI_INT, 
						settings->md_ranks[i], 
						MPI_ANY_TAG, 
						settings->comm_group, 
						&status);

			rc |= (status.MPI_TAG == REGISTER_JOB_SUCCESS) ? 0 : -1;
		}
	} else {
		rc = -1;
	}

	return rc;
}

int SPDC_Finalize_Registration() {
	MPI_Status status;

	SPDC_HDFS_Job dummy;
	int dummy2;

	for(int i = 0; i < settings->num_md_servers; i++) {
		MPI_Send(	&dummy, 
					sizeof(SPDC_HDFS_Job), 
					MPI_BYTE, 
					settings->md_ranks[i], 
					JOB_REGISTRATION_FINISHED, 
					settings->comm_group);

		MPI_Recv(	&dummy2, 
					1, 
					MPI_INT, 
					settings->md_ranks[i], 
					JOB_FIN_REGISTRATION_CONF, 
					settings->comm_group, 
					&status);		
	}

	return 0;
}

int SPDC_Master_Init() {
	return 0;
}

int SPDC_Init_MD_Server() {
	//	Initialize Job Vector
	job_vector = new vector<SPDC_HDFS_Job*>;
	
	//	Sort ranks and find our ranks position
	vector<int> sorted_ranks;
	sorted_ranks.assign(settings->md_ranks, settings->md_ranks + settings->num_md_servers);
	sort(sorted_ranks.begin(), sorted_ranks.end());
	
	md_id = find(sorted_ranks.begin(), sorted_ranks.end(), our_rank) - sorted_ranks.begin();
	//char msg[200];

	//sprintf(msg, "Building Jobs");
	//SPDC_Debug_Message(msg);

	SPDC_Build_Initial_Jobs();
	//sprintf(msg, "Bulding locality map");
	//SPDC_Debug_Message(msg);
	SPDC_Receive_Slave_Checkins();
	SPDC_Distribute_Slave_Checkins();
	SPDC_Receive_Slave_Checkin_Dist();

	SPDC_Build_Locality_Map();

	//fprintf(stdout, "DEBUG  ---  rank: %d done building\n", our_rank);

	//sprintf(msg, "Distributing locality map");
	//SPDC_Debug_Message(msg);
	SPDC_Distribute_Locality_Map();

	//fprintf(stdout, "DEBUG  ---  rank: %d done with distro\n", our_rank);

	//sprintf(msg, "Receiving locality map");
	//SPDC_Debug_Message(msg);
	SPDC_Receive_Locality_Map();

	//fprintf(stdout, "DEBUG  ---  rank: %d done receiving other locality maps\n", our_rank);

	//sprintf(msg, "Receiving slave checkins");
	//SPDC_Debug_Message(msg);



	//sprintf(msg, "Done Receiving slave checkins");
	//SPDC_Debug_Message(msg);
	//fprintf(stdout, "DEBUG  ---  rank: %d done receiving slave checkins\n", our_rank);

	//print_slave_hostname(slave_hostname_vector);


	//sprintf(msg, "Done Distributing slave checkins");
	//SPDC_Debug_Message(msg);
	//fprintf(stdout, "DEBUG  ---  rank: %d done distributing slaves\n", our_rank);

	//sprintf(msg, "Done Receiving slave checkin distribution");
	//SPDC_Debug_Message(msg);

	//fprintf(stdout, "DEBUG  ---  rank: %d done receiving slave dist\n", our_rank);

	SPDC_Build_Chunk_Job_Map();
	//sprintf(msg, "Done Building Chunk Map");
	//SPDC_Debug_Message(msg);
	//fprintf(stdout, "DEBUG  ---  rank: %d done building job chunk map\n", our_rank);

	SPDC_Distribute_Slave_Jobs();

	//sprintf(msg, "Done distributing slave jobs");
	//SPDC_Debug_Message(msg);
	//fprintf(stdout, "DEBUG  ---  rank: %d done distributing slave jobs\n", our_rank);
/*
	struct timeval tv;
	gettimeofday(&tv, NULL);
	double start = (tv.tv_sec) * 1000.0 + (tv.tv_usec) / 1000.0;
*/
	SPDC_MD_Server();
/*
	gettimeofday(&tv, NULL);
	double end = (tv.tv_sec) * 1000.0 + (tv.tv_usec) / 1000.0;

	char msg[200];
	sprintf(msg, "MD Server end, took %f", (end - start));
	SPDC_Debug_Message(msg);
*/
	return 0;
}

int SPDC_Init_Slave() {
	//fprintf(stdout, "DEBUG  ---  rank: %d initializing as slave\n", our_rank);
	char msg[200];
	//sprintf(msg, "Beginning slave Initialization");
	//SPDC_Debug_Message(msg);

	//	Sort slave ranks and get our position
	vector<int> sorted_ranks;
	sorted_ranks.assign(settings->slave_ranks, settings->slave_ranks + settings->num_slaves);
	sort(sorted_ranks.begin(), sorted_ranks.end());
	sl_id = find(sorted_ranks.begin(), sorted_ranks.end(), our_rank) - sorted_ranks.begin();

	int w = settings->num_slaves / settings->num_md_servers;
	md_id = sl_id / w;
	
	if(md_id >= settings->num_md_servers) md_id = settings->num_md_servers - 1;  

	//sprintf(msg, "MDid = %d", md_id);
	//SPDC_Debug_Message(msg);

	//	Sort md servers and get our assosciated md rank
	sorted_md_ranks = new vector<int>;
	sorted_md_ranks->assign(settings->md_ranks, settings->md_ranks + settings->num_md_servers);
	sort(sorted_md_ranks->begin(), sorted_md_ranks->end());

	md_rank = sorted_md_ranks->at(md_id);

	//	Send our hostname to owner md
	char* hostname = (char*) calloc(MAX_HOSTNAME_SIZE, sizeof(char));
	gethostname(hostname, MAX_HOSTNAME_SIZE);

	MPI_Send(	hostname,
				strlen(hostname),
				MPI_CHAR,
				md_rank,
				SLAVE_CHECK_IN,
				settings->comm_group);

	//fprintf(stdout, "DEBUG  ---  rank: %d sent checkin to %d\n", our_rank, md_rank);
	sprintf(msg, "Sent checkin to MDS %d", md_rank);
	SPDC_Debug_Message(msg);

	int dummy;
	MPI_Status status;

	MPI_Recv(	&dummy,
				1,
				MPI_INT,
				md_rank,
				SLAVE_CHECK_IN_RESP,
				settings->comm_group,
				&status);


	SPDC_Slave_Receive_Init_Jobs();
	SPDC_Slave_Sort_Jobs();
	//SPDC_Debug_Print_Jobs();

	done_jobs = new vector<SPDC_HDFS_Job*>;

	return 0;
}

void SPDC_Slave_Sort_Jobs() {
	sort(job_vector->begin(), job_vector->end(), SPDC_Compare_Job);
}


void SPDC_Slave_Receive_Init_Jobs() {
	MPI_Status status;
	int dummy;

	SPDC_Slave_Init_Chunks();
	
	job_vector = new vector<SPDC_HDFS_Job*>;

	while(1) {
		MPI_Probe(	md_rank, 
					MPI_ANY_TAG, 
					settings->comm_group, 
					&status);

		if(status.MPI_TAG == BEGIN_SLAVE_JOB_DIST) {
			SPDC_HDFS_Job *new_job = (SPDC_HDFS_Job*) calloc(1, sizeof(SPDC_HDFS_Job));

			MPI_Recv(	new_job,
						sizeof(SPDC_HDFS_Job),
						MPI_BYTE,
						md_rank,
						BEGIN_SLAVE_JOB_DIST,
						settings->comm_group,
						&status);

			new_job->filename = (char*) calloc(new_job->filename_length, sizeof(char));

			MPI_Recv(	new_job->filename,
						new_job->filename_length,
						MPI_CHAR,
						md_rank,
						SLAVE_JOB_DIST_FILENAME,
						settings->comm_group,
						&status);		

			new_job->included_chunks = (int*) calloc(new_job->num_included_chunks, sizeof(int));

			MPI_Recv(	new_job->included_chunks,
						new_job->num_included_chunks,
						MPI_INT,
						md_rank,
						SLAVE_JOB_DIST_CHUNKS,
						settings->comm_group,
						&status);

			job_vector->push_back(new_job);

		} else if(status.MPI_TAG == END_SLAVE_JOB_DIST) {
			MPI_Recv(	&dummy,
						1,
						MPI_INT,
						md_rank,
						END_SLAVE_JOB_DIST,
						settings->comm_group,
						&status);
			break;
		}
	}
}

void SPDC_Slave_Init_Chunks() {
	MPI_Status status;
	int dummy;

	slave_info = (SPDC_HDFS_Slave_Info*) calloc(1, sizeof(SPDC_HDFS_Slave_Info));

	MPI_Probe(	md_rank, 
				MPI_ANY_TAG, 
				settings->comm_group, 
				&status);

	if(status.MPI_TAG == SLAVE_INIT_NUM_CHUNKS) {
		MPI_Recv(	&slave_info->num_chunks,
					1,
					MPI_INT,
					md_rank,
					SLAVE_INIT_NUM_CHUNKS,
					settings->comm_group,
					&status);

		slave_info->chunks = (int*) calloc(slave_info->num_chunks, sizeof(int));

		MPI_Recv(	slave_info->chunks,
					slave_info->num_chunks,
					MPI_INT,
					md_rank,
					SLAVE_INIT_LOC_CHUNKS,
					settings->comm_group,
					&status);

	} else if (status.MPI_TAG == SLAVE_INIT_CHUNKS_NONE) {
		//fprintf(stdout, "DEBUG  ---  rank: %d has no local chunks :(\n", our_rank);

		MPI_Recv(	&dummy,
					1,
					MPI_INT,
					md_rank,
					SLAVE_INIT_CHUNKS_NONE,
					settings->comm_group,
					&status);
	}
}


void SPDC_Build_Chunk_Job_Map() {
	for(uint i = 0; i < job_vector->size(); i++) {
		SPDC_HDFS_Job *job = job_vector->at(i);
		SPDC_HDFS_File_Info *fi = get_file_info(job->filename, file_info_vector);

		uint64_t chunk_size = fi->chunk_size;

		uint64_t start_chunk = job->start_offset / chunk_size;
		uint64_t end_chunk = (job->start_offset + job->length) / chunk_size;

		int num_chunks = end_chunk - start_chunk + 1;
		job->num_included_chunks = num_chunks;
		job->included_chunks = (int*) calloc(num_chunks, sizeof(int));

		for(int j = 0; j < num_chunks; j++) {
			job->included_chunks[j] = start_chunk+j;
		}
	}

	//char msg[200];
	fprintf(stdout, "***%d Done build chunk job map\n", our_rank);
	//SPDC_Debug_Message(msg);
}

void SPDC_Receive_Slave_Checkins() {
	int work = settings->num_slaves / settings->num_md_servers;
	num_slaves_resp = work;

	//	If this is the last md and there is an uneven distribution we'll take it
	if(	md_id == settings->num_md_servers - 1 && 
		settings->num_slaves % settings->num_md_servers) num_slaves_resp+=settings->num_slaves % settings->num_md_servers;

	//fprintf(stdout, "DEBUG  ---  rank: %d receiving slave checking, responsible for %d slaves\n", our_rank, num_slaves_resp);

	//	Sort slaves
	vector<int> sorted_slaves;
	sorted_slaves.assign(settings->slave_ranks, settings->slave_ranks + settings->num_slaves);
	sort(sorted_slaves.begin(), sorted_slaves.end());

	//	Calculate start and allocate mem for slaves array
	int start = md_id * work;
	slaves = (int*) calloc(num_slaves_resp, sizeof(int));
	
	//	Copy into slaves array
	memcpy(slaves, sorted_slaves.data()+start, sizeof(int)*num_slaves_resp);

	slave_hostname_vector = new vector<SPDC_Hostname_Rank*>;

	int num_done = 0;
	char recv_hostname[MAX_HOSTNAME_SIZE];
	MPI_Status status;
	while (num_done < num_slaves_resp) {
		SPDC_Hostname_Rank* new_hnr;

		//fprintf(stdout, "DEBUG  ---  rank: %d waiting for checking, %d so far\n", our_rank, num_done);

		memset(recv_hostname, 0, MAX_HOSTNAME_SIZE);

		//	Receive a hostname
		MPI_Recv(	recv_hostname,
					MAX_HOSTNAME_SIZE,
					MPI_CHAR,
					MPI_ANY_SOURCE,
					SLAVE_CHECK_IN,
					settings->comm_group,
					&status);

		//	If we have already received this hostname, add the rank to the hnr struct
		if((new_hnr = find_hnr_from_hostname(recv_hostname, slave_hostname_vector)) != NULL) {
			new_hnr->num_ranks++;
			int* new_ranks = (int*) calloc(new_hnr->num_ranks, sizeof(int));
			memcpy(new_ranks, new_hnr->ranks, sizeof(int)*(new_hnr->num_ranks-1));
			free(new_hnr->ranks);
			new_hnr->ranks = new_ranks;
		} else {
			//	Allocate memory for struct and name
			new_hnr = (SPDC_Hostname_Rank*) calloc(1, sizeof(SPDC_Hostname_Rank));
			new_hnr->hostname = (char*) calloc(strlen(recv_hostname)+1, sizeof(char));
			new_hnr->ranks = (int*) calloc(1, sizeof(int));
			//	Copy hostname and rank
			//memcpy(new_hnr, recv_hostname, strlen(recv_hostname)); doesnt work for some reason ...
			strcpy(new_hnr->hostname, recv_hostname);

			new_hnr->ranks[0] = status.MPI_SOURCE;
			new_hnr->num_ranks = 1;

			slave_hostname_vector->push_back(new_hnr);
		}

		MPI_Send(	&num_done,
					1,
					MPI_INT,
					status.MPI_SOURCE,
					SLAVE_CHECK_IN_RESP,
					settings->comm_group);

		num_done++;
	}
}

int SPDC_Build_Initial_Jobs() {
	MPI_Status status;

	SPDC_HDFS_Job* recv_job;
	recv_job = (SPDC_HDFS_Job*) calloc(1, sizeof(SPDC_HDFS_Job));

	int dummy, rc;

	while(1) {
		//	Receive a Job Message
		MPI_Recv(	recv_job,
					sizeof(SPDC_HDFS_Job),
					MPI_BYTE,
					settings->master_rank,
					MPI_ANY_TAG,
					settings->comm_group,
					&status);

		//	If it is registration
		if(status.MPI_TAG == REGISTER_JOB) {
			//	Create a new job pointer, allocate memory for it and copy from recv_job
			SPDC_HDFS_Job* insert_job;
			insert_job = (SPDC_HDFS_Job*) calloc(1, sizeof(SPDC_HDFS_Job));
			memcpy(insert_job, recv_job, sizeof(SPDC_HDFS_Job));

			//	Send success
			MPI_Send(	&dummy,
						1,
						MPI_INT,
						settings->master_rank,
						REGISTER_JOB_SUCCESS,
						settings->comm_group);


			//	allocate memory for the filename
			insert_job->filename = (char*) calloc(MAX_FILENAME_SIZE, sizeof(char));

			//	Receive the assosciated filename
			MPI_Recv(	insert_job->filename,
						MAX_FILENAME_SIZE,
						MPI_CHAR,
						settings->master_rank,
						REGISTER_FILENAME,
						settings->comm_group,
						&status);

			//	Should validate id here.  Will do later
			job_vector->push_back(insert_job);

			//	Send final success
			MPI_Send(	&dummy,
						1,
						MPI_INT,
						settings->master_rank,
						REGISTER_JOB_SUCCESS,
						settings->comm_group);

		} else if(status.MPI_TAG == JOB_REGISTRATION_FINISHED) {
			//	Send finalization confirmation
			MPI_Send(	&dummy,
						1,
						MPI_INT,
						settings->master_rank,
						JOB_FIN_REGISTRATION_CONF,
						settings->comm_group);
			rc = 0;
			break;
		}
	}

	free(recv_job);
	return rc;
}

int SPDC_Check_Finished() {
	int flag = 0;
	int r = 0;
	MPI_Status status;

	MPI_Iprobe(	MPI_ANY_SOURCE,
				SLAVE_FINALIZATION,
				settings->comm_group,
				&flag,
				&status);

	while(flag) {
		int dummy;
		MPI_Recv(	&dummy,
					1,
					MPI_INT,
					status.MPI_SOURCE,
					SLAVE_FINALIZATION,
					settings->comm_group,
					&status);

		flag = 0;
		r++;

		MPI_Iprobe(	MPI_ANY_SOURCE,
					SLAVE_FINALIZATION,
					settings->comm_group,
					&flag,
					&status);
	}

	return r;
}

int SPDC_Check_MD_Finished() {
	int flag = 0;
	int r = 0;
	MPI_Status status;
	
	MPI_Iprobe(	MPI_ANY_SOURCE,
				MD_FINALIZATION,
				settings->comm_group,
				&flag,
				&status);

	while(flag) {
		int dummy;
		MPI_Recv(	&dummy,
					1,
					MPI_INT,
					status.MPI_SOURCE,
					MD_FINALIZATION,
					settings->comm_group,
					&status);

		flag = 0;
		r++;

		MPI_Iprobe(	MPI_ANY_SOURCE,
					MD_FINALIZATION,
					settings->comm_group,
					&flag,
					&status);
	}

	return r;	
}

int SPDC_MD_Server() {
	request_queue = new vector<SPDC_HDFS_Job_Request*>;
	struct timeval tv;
	gettimeofday(&tv, NULL);
	double start = (tv.tv_sec) * 1000.0 + (tv.tv_usec) / 1000.0;
	
	//char msg[200];
	//fprintf(stdout, "***%d Beginning server\n", our_rank);
	//SPDC_Debug_Message(msg);

	int num_done = 0;
	//struct timeval  tv;
	//char msg[200];

	while(num_done < num_slaves_resp) {
		num_changed_jobs = 0;

		//gettimeofday(&tv, NULL);
		//double start_time = (tv.tv_sec) * 1000 + (tv.tv_usec) / 1000 ;

		SPDC_Receive_Job_Requests();
		//sprintf(msg, "Received %u requests", request_queue->size());
		//SPDC_Debug_Message(msg);

		SPDC_Send_Request_Response();
		
		SPDC_Send_Job_Update();

		//sprintf(msg, "issued responses");
		//SPDC_Debug_Message(msg);
		num_done += SPDC_Check_Finished();

		//gettimeofday(&tv, NULL);
		//double end_time = (tv.tv_sec) * 1000 + (tv.tv_usec) / 1000 ;
/*
		if(num_changed_jobs > 0) {
			sprintf(msg, "\t\t*****Cycle took %f****", (end_time - start_time));
			SPDC_Debug_Message(msg);
		}
*/
	}

	num_done = 1;
	int dummy;

	for(int i = 0; i < settings->num_md_servers; i++) {
		if(settings->md_ranks[i] != our_rank) {
			MPI_Send(	&dummy,
						1,
						MPI_INT,
						settings->md_ranks[i],
						MD_FINALIZATION,
						settings->comm_group);
		}
	}

	while(num_done < settings->num_md_servers) {

		SPDC_Send_Request_Resolutions();
		//sprintf(msg, "Sent Request Resolutions");
		//SPDC_Debug_Message(msg);

		SPDC_Receive_Request_Resolution();
		//sprintf(msg, "Received resolutions");
		//SPDC_Debug_Message(msg);

		num_done += SPDC_Check_MD_Finished();
	}

	gettimeofday(&tv, NULL);
	double end = (tv.tv_sec) * 1000.0 + (tv.tv_usec) / 1000.0;

	char msg[200];
	sprintf(msg, "MD Server end, took %f", (end - start));
	SPDC_Debug_Message(msg);

	SPDC_MD_Finalize();
	return 0;
}

void SPDC_Send_Job_Update() {
	if(num_changed_jobs > 0) {
		for(int i = 0; i < num_slaves_resp; i++) {
			MPI_Send(	&num_changed_jobs,
						1,
						MPI_INT,
						slaves[i],
						SLAVE_JOB_UPDATE,
						settings->comm_group);

			MPI_Send(	changed_jobs,
						num_changed_jobs,
						MPI_INT,
						slaves[i],
						SLAVE_SEND_UPDATED_JOBS,
						settings->comm_group);
		}		
		free(changed_jobs);
	}
}

void SPDC_MD_Finalize() {
	int dummy;

	if(settings->debug_mode) {
		MPI_Send(	&dummy,
					1,
					MPI_INT,
					settings->debug_rank,
					DEBUG_FINALIZATION,
					settings->comm_group);
	}
}

void SPDC_Send_Request_Response() {
	int dummy;
	while(request_queue->size() > 0) {
		SPDC_HDFS_Job_Request* request = request_queue->back();
		request_queue->pop_back();

		dummy = 1;

		//char msg[200];
		//sprintf(msg, "size: %lu, id %d, req %d", job_vector->size(), request->job_id, request->requester);
		//SPDC_Debug_Message(msg); 

		if(job_vector->at(request->job_id)->status == UN_ALLOCATED &&
			request->valid) {
			
			//char msg[200];
			//sprintf(msg, "Sending good response to slave %d for job %d", request->requester, request->job_id);
			//SPDC_Debug_Message(msg);

			MPI_Send(	&dummy,
						1,
						MPI_INT,
						request->requester,
						JOB_REQUEST_RESPONSE,
						settings->comm_group);
			job_vector->at(request->job_id)->status = ALLOCATED;

			num_changed_jobs++;

			if(num_changed_jobs > 1) {
				int* new_jobs = (int*) calloc(num_changed_jobs, sizeof(int));
				memcpy(new_jobs, changed_jobs, sizeof(int)*(num_changed_jobs-1));
				free(changed_jobs);
				changed_jobs = new_jobs;
				new_jobs[num_changed_jobs-1] = job_vector->at(request->job_id)->id;
			} else {
				changed_jobs = (int*) calloc(1, sizeof(int));
				changed_jobs[0] = job_vector->at(request->job_id)->id;
			}

		} else {
			dummy = 0;
			
			//char msg[200];
			//sprintf(msg, "Sending bad response to slave %d for job %d, already allocated", request->requester, request->job_id);
			//SPDC_Debug_Message(msg);
			
			MPI_Send(	&dummy,
						1,
						MPI_INT,
						request->requester,
						JOB_REQUEST_RESPONSE,
						settings->comm_group);			
		}

		free(request);
	}
}

void SPDC_Send_Request_Resolutions() {
	for(uint i = 0; i < request_queue->size(); i++) {
		SPDC_HDFS_Job_Request* request = request_queue->at(i);

		for(int j = 0; j < settings->num_md_servers; j++) {
			
			if(settings->md_ranks[j] != our_rank) {
				//char msg[200];
				//sprintf(msg, "Sending Request Resolution to md server %d for job %d", settings->md_ranks[j], request->job_id);
				//SPDC_Debug_Message(msg);

				MPI_Send(	request,
							sizeof(SPDC_HDFS_Job_Request),
							MPI_BYTE,					
							settings->md_ranks[j],
							JOB_REQUEST_RESOLUTION,
							settings->comm_group);
			}
		}
	}

	for(int j = 0; j < settings->num_md_servers; j++) {
		int dummy;
		if(settings->md_ranks[j] != our_rank) {
			MPI_Send(	&dummy,
						1,
						MPI_INT,
						settings->md_ranks[j],
						JOB_REQUEST_RESOLUTION_FINISHED,
						settings->comm_group);
		}
	}
}

void SPDC_Receive_Request_Resolution() {
	MPI_Status status;

	for(int i = 0; i < settings->num_md_servers; i++) {
		if(settings->md_ranks[i] != our_rank) {
			while(1) {
				MPI_Probe(	settings->md_ranks[i],
							MPI_ANY_TAG,
							settings->comm_group,
							&status);

				if(status.MPI_TAG == JOB_REQUEST_RESOLUTION) {
					SPDC_HDFS_Job_Request recv_request;
					MPI_Recv(	&recv_request,
								sizeof(SPDC_HDFS_Job_Request),
								MPI_BYTE,
								settings->md_ranks[i],
								JOB_REQUEST_RESOLUTION,
								settings->comm_group,
								&status);

					//char msg[200];
					//sprintf(msg, "Received Request resolution for job %d from md server %d", recv_request.job_id, settings->md_ranks[i]);
					//SPDC_Debug_Message(msg);

					SPDC_Resolve_Request(recv_request);

				} else {
					int dummy;
					MPI_Recv(	&dummy,
								1,
								MPI_INT,
								settings->md_ranks[i],
								JOB_REQUEST_RESOLUTION_FINISHED,
								settings->comm_group,
								&status);					
					break;
				}
			}
		}
	}
}

void SPDC_Resolve_Request(SPDC_HDFS_Job_Request recv_request) {
	vector<SPDC_HDFS_Job_Request*> remove_vector;
	int found = 0;

	for(uint i = 0; i < request_queue->size(); i++) {
		//	If we find a matching request
		if(recv_request.job_id == request_queue->at(i)->job_id) {
			//char msg[200];

			//sprintf(msg, "Found conflict from slave %d for job %d", recv_request.requester, recv_request.job_id);
			//SPDC_Debug_Message(msg);

			//	If we should not get the job remove it from our request queue and set status to allocated
			if(recv_request.job_id % settings->num_md_servers != md_id) {
				//sprintf(msg, "Slave %d Not getting job %d", recv_request.requester, recv_request.job_id);
				//SPDC_Debug_Message(msg);
				
				job_vector->at(request_queue->at(i)->job_id)->status = ALLOCATED;
				request_queue->at(i)->valid = 0;

				num_changed_jobs++;

				if(num_changed_jobs > 1) {
					int* new_jobs = (int*) calloc(num_changed_jobs, sizeof(int));
					memcpy(new_jobs, changed_jobs, sizeof(int)*(num_changed_jobs-1));
					free(changed_jobs);
					changed_jobs = new_jobs;
					new_jobs[num_changed_jobs-1] = request_queue->at(i)->job_id;
				} else {
					changed_jobs = (int*) calloc(1, sizeof(int));
					changed_jobs[0] = request_queue->at(i)->job_id;
				}

				//remove_vector.push_back(request_queue->at(i));
			}

			found = 1;
			break;
		}
	}

	//	If we did not find it, set to allocated
	if(!found) {
		job_vector->at(recv_request.job_id)->status = ALLOCATED;

		num_changed_jobs++;

		if(num_changed_jobs > 1) {
			int* new_jobs = (int*) calloc(num_changed_jobs, sizeof(int));
			memcpy(new_jobs, changed_jobs, sizeof(int)*(num_changed_jobs-1));
			free(changed_jobs);
			changed_jobs = new_jobs;
			new_jobs[num_changed_jobs-1] = recv_request.job_id;
		} else {
			changed_jobs = (int*) calloc(1, sizeof(int));
			changed_jobs[0] = recv_request.job_id;
		}

	}
/*
	for(uint i = 0; i < remove_vector.size(); i++) {
		SPDC_HDFS_Job_Request *request = remove_vector.at(i);
		int dummy = 0;
		char msg[200];
		sprintf(msg, "Conflicting request from slave %d for job %d", request->requester, request->job_id);
		SPDC_Debug_Message(msg);

		MPI_Send(	&dummy,
					1,
					MPI_INT,
					request->requester,
					JOB_REQUEST_RESPONSE,
					settings->comm_group);


		//	I think this is causing some problems ...  
		remove(request_queue->begin(), request_queue->end(), remove_vector.at(i));
		free(remove_vector.at(i));
	}
*/
}

void SPDC_Receive_Job_Requests() {
	MPI_Status status;
	int flag;

	uint t = (job_vector->size() / settings->num_md_servers);
	uint low_range = md_id * t;
	uint high_range = (md_id + 1) * t;

	while(1) {

		MPI_Iprobe(	MPI_ANY_SOURCE,
					IS_JOB_AVAILABLE,
					settings->comm_group,
					&flag,
					&status);
	
		if(flag) {
			uint id;

			MPI_Recv(	&id,
						1,
						MPI_INT,
						status.MPI_SOURCE,
						IS_JOB_AVAILABLE,
						settings->comm_group,
						&status);

			if(id >= low_range && id < high_range && 
				job_vector->size() > id && 
				(job_vector->at(id)->status == UN_ALLOCATED)) {
				SPDC_HDFS_Job_Request* request = (SPDC_HDFS_Job_Request*) calloc(1, sizeof(SPDC_HDFS_Job_Request));
				request->requester = status.MPI_SOURCE;
				request->job_id = id;
				request->status = UN_ALLOCATED;
				request->valid = 1;

				request_queue->push_back(request);

				//char msg[200];
				//sprintf(msg, "Received valid request for job %d from slave %d", request->job_id, request->requester);
				//SPDC_Debug_Message(msg);

			} else {
				int r = 0;
				
				//char msg[200];
				//sprintf(msg, "Received valid request for job %d from slave %d, but job already allocated", id, status.MPI_SOURCE);
				//SPDC_Debug_Message(msg);

				MPI_Send(	&r,
							1,
							MPI_INT,
							status.MPI_SOURCE,
							JOB_REQUEST_RESPONSE,
							settings->comm_group);
			}
			
		} else {
			break;
		}
	}
}

void SPDC_Distribute_Slave_Jobs() {
	int dummy;
	
	SPDC_Distribute_Slave_Chunks();

	for(uint i = 0; i < job_vector->size(); i++) {
		for(int j = 0; j < num_slaves_resp; j++) {
			SPDC_Send_Slave_Job(job_vector->at(i), slaves[j]);
		}
	}

	for(int i = 0; i < num_slaves_resp; i++) {
		MPI_Send(	&dummy,
					1,
					MPI_INT,
					slaves[i],
					END_SLAVE_JOB_DIST,
					settings->comm_group);
	}
}

void SPDC_Send_Slave_Job(SPDC_HDFS_Job* job, int slave_rank) {
	MPI_Send(	job,
				sizeof(SPDC_HDFS_Job),
				MPI_BYTE,
				slave_rank,
				BEGIN_SLAVE_JOB_DIST,
				settings->comm_group);

	MPI_Send(	job->filename,
				job->filename_length,
				MPI_CHAR,
				slave_rank,
				SLAVE_JOB_DIST_FILENAME,
				settings->comm_group);

	MPI_Send(	job->included_chunks,
				job->num_included_chunks,
				MPI_INT,
				slave_rank,
				SLAVE_JOB_DIST_CHUNKS,
				settings->comm_group);

}

void SPDC_Distribute_Slave_Chunks() {
	int dummy;
	char msg[200];

	for(uint i = 0; i < locality_vector->size(); i++) {
		//int* slave_ranks;
		//int num_ranks = get_ranks_from_hostname(locality_vector->at(i)->hostname, slave_hostname_vector, slave_ranks);
		SPDC_Hostname_Rank *hnr = find_hnr_from_hostname(locality_vector->at(i)->hostname, slave_hostname_vector);

		if(hnr != NULL) {
			locality_vector->at(i)->ranks = hnr->ranks;			
			locality_vector->at(i)->num_ranks = hnr->num_ranks;
		}
		else {
			locality_vector->at(i)->num_ranks = 0;
			continue;
		}
	}

	for(int i = 0; i < num_slaves_resp; i++) {
		SPDC_HDFS_Host_Chunk_Map* m = get_chunk_map_from_rank(slaves[i], locality_vector);
		
		if(m == NULL) {
			MPI_Send(	&dummy,
						1,
						MPI_INT,
						slaves[i],
						SLAVE_INIT_CHUNKS_NONE,
						settings->comm_group);	
			continue;
		}

		int* chunks = m->chunks;
		int num_chunks = m->num_chunks;

		MPI_Send(	&num_chunks,
					1,
					MPI_INT,
					slaves[i],
					SLAVE_INIT_NUM_CHUNKS,
					settings->comm_group);		

		MPI_Send(	chunks,
					num_chunks,
					MPI_INT,
					slaves[i],
					SLAVE_INIT_LOC_CHUNKS,
					settings->comm_group);
	}
}

void SPDC_Distribute_Slave_Checkins() {
	SPDC_Hostname_Rank* temp;
	int dummy;

	for(uint i = 0; i < slave_hostname_vector->size(); i++) {
		temp = slave_hostname_vector->at(i);
		//fprintf(stdout, "DEBUG  ---  rank: %d distributing slave# %d\n", our_rank, temp->rank);

		for(int j = 0; j < settings->num_md_servers; j++) {
			if(settings->md_ranks[j] != our_rank) {
				MPI_Send(	temp,
							sizeof(SPDC_Hostname_Rank),
							MPI_BYTE,
							settings->md_ranks[j],
							MD_SLAVE_ASS_DIST,
							settings->comm_group);

				MPI_Send(	temp->hostname,
							strlen(temp->hostname),
							MPI_CHAR,
							settings->md_ranks[j],
							MD_SLAVE_ASS_DIST_HOSTNAME,
							settings->comm_group);

				MPI_Send(	temp->ranks,
							temp->num_ranks,
							MPI_INT,
							settings->md_ranks[j],
							MD_SLAVE_ASS_DIST_RANKS,
							settings->comm_group);
			
				if(i+1 < slave_hostname_vector->size()) {
					//fprintf(stdout, "DEBUG  ---  rank: %d sending more on slave# %d\n", our_rank, i);
					MPI_Send(	&dummy,
								1,
								MPI_INT,
								settings->md_ranks[j],
								MD_SLAVE_ASS_DIST_MORE,
								settings->comm_group);
				}
			}
		}
	}

	//fprintf(stdout, "DEBUG  ---  rank: %d sending slave distribution finalizations\n", our_rank);
	for(int j = 0; j < settings->num_md_servers; j++) {
		if(settings->md_ranks[j] != our_rank) {
			MPI_Send(	&dummy,
						1,
						MPI_INT,
						settings->md_ranks[j],
						MD_SLAVE_ASS_DIST_DONE,
						settings->comm_group);
		}
	}
}

void SPDC_Receive_Slave_Checkin_Dist() {
	MPI_Status status;
	SPDC_Hostname_Rank *recv = (SPDC_Hostname_Rank*) calloc(1, sizeof(SPDC_Hostname_Rank));
	int dummy;

	for(int i = 0; i < settings->num_md_servers; i++) {
		if(settings->md_ranks[i] != our_rank) {
			while(1) {

				MPI_Recv(	recv,
							sizeof(SPDC_Hostname_Rank),
							MPI_BYTE,
							settings->md_ranks[i],
							MD_SLAVE_ASS_DIST,
							settings->comm_group,
							&status);

				SPDC_Hostname_Rank *new_hnr = (SPDC_Hostname_Rank*) calloc(1, sizeof(SPDC_Hostname_Rank));
				memcpy(new_hnr, recv, sizeof(SPDC_Hostname_Rank));

				new_hnr->hostname = (char*) calloc(MAX_HOSTNAME_SIZE, sizeof(char));
				new_hnr->ranks = (int*) calloc(new_hnr->num_ranks, sizeof(int));

				MPI_Recv(	new_hnr->hostname,
							MAX_HOSTNAME_SIZE,
							MPI_CHAR,
							status.MPI_SOURCE,
							MD_SLAVE_ASS_DIST_HOSTNAME,
							settings->comm_group,
							&status);

				MPI_Recv(	new_hnr->ranks,
							new_hnr->num_ranks,
							MPI_INT,
							status.MPI_SOURCE,
							MD_SLAVE_ASS_DIST_RANKS,
							settings->comm_group,
							&status);

				slave_hostname_vector->push_back(new_hnr);

				MPI_Probe(	status.MPI_SOURCE, 
							MPI_ANY_TAG, 
							settings->comm_group, 
							&status);

				if(status.MPI_TAG == MD_SLAVE_ASS_DIST_DONE) {
					MPI_Recv(	&dummy,
								1,
								MPI_INT,
								status.MPI_SOURCE,
								MD_SLAVE_ASS_DIST_DONE,
								settings->comm_group,
								&status);
					break;
				} else {
					MPI_Recv(	&dummy,
								1,
								MPI_INT,
								status.MPI_SOURCE,
								MD_SLAVE_ASS_DIST_MORE,
								settings->comm_group,
								&status);					
				}
			}

			//fprintf(stdout, "DEBUG  ---  rank: %d received slave dist finalization from %d\n", our_rank, status.MPI_SOURCE);
		}
	}

	free(recv);
}

void SPDC_Distribute_Locality_Map() {
	SPDC_HDFS_Host_Chunk_Map* temp_map;

	int dummy;

	for(uint i = 0; i < locality_vector->size(); i++) {
		temp_map = locality_vector->at(i);
		//fprintf(stdout, "DEBUG  ---  rank: %d distributing li# %d\n", our_rank, i);
		for(int j = 0; j < settings->num_md_servers; j++) {
			if(settings->md_ranks[j] != our_rank) {
				MPI_Send(	temp_map,
							sizeof(SPDC_HDFS_Host_Chunk_Map),
							MPI_BYTE,
							settings->md_ranks[j],
							MD_LOCALITY_DISTRIBUTION,
							settings->comm_group);

				MPI_Send(	temp_map->hostname,
							strlen(temp_map->hostname),
							MPI_CHAR,
							settings->md_ranks[j],
							MD_LOCALITY_DIST_HOSTNAME,
							settings->comm_group);
				
				MPI_Send(	temp_map->chunks,
							temp_map->num_chunks,
							MPI_INT,
							settings->md_ranks[j],
							MD_LOCALITY_DIST_CHUNKS,
							settings->comm_group);

				if(i+1 < locality_vector->size()) {
					//fprintf(stdout, "DEBUG  ---  rank: %d sending more on li# %d\n", our_rank, i);
					MPI_Send(	&dummy,
								1,
								MPI_INT,
								settings->md_ranks[j],
								MD_LOCALITY_DIST_MORE,
								settings->comm_group);
				}

			}
		}
	}

	//fprintf(stdout, "DEBUG  ---  rank: %d sending build finalizations\n", our_rank);
	for(int j = 0; j < settings->num_md_servers; j++) {
		if(settings->md_ranks[j] != our_rank) {
			MPI_Send(	&dummy,
						1,
						MPI_INT,
						settings->md_ranks[j],
						MD_LOCALITY_DIST_DONE,
						settings->comm_group);
		}
	}
}

void SPDC_Receive_Locality_Map() {
	MPI_Status status;
	SPDC_HDFS_Host_Chunk_Map *recv_map = (SPDC_HDFS_Host_Chunk_Map*) calloc(1, sizeof(SPDC_HDFS_Host_Chunk_Map));
	int dummy;
	//int num_done = 0;

	for(int i = 0; i < settings->num_md_servers; i++) {
		if(settings->md_ranks[i] != our_rank) {
			while(1) {

				MPI_Probe(	settings->md_ranks[i], 
							MPI_ANY_TAG, 
							settings->comm_group, 
							&status);

				if(status.MPI_TAG != MD_LOCALITY_DIST_DONE) {
					MPI_Recv(	recv_map,
								sizeof(SPDC_HDFS_Host_Chunk_Map),
								MPI_BYTE,
								settings->md_ranks[i],
								MD_LOCALITY_DISTRIBUTION,
								settings->comm_group,
								&status);

					char* hostname = (char*) calloc(MAX_HOSTNAME_SIZE, sizeof(char));
					int* chunks = (int*) calloc(recv_map->num_chunks, sizeof(int));

					MPI_Recv(	hostname,
								MAX_HOSTNAME_SIZE,
								MPI_CHAR,
								status.MPI_SOURCE,
								MD_LOCALITY_DIST_HOSTNAME,
								settings->comm_group,
								&status);

					MPI_Recv(	chunks,
								recv_map->num_chunks,
								MPI_INT,
								status.MPI_SOURCE,
								MD_LOCALITY_DIST_CHUNKS,
								settings->comm_group,
								&status);

					SPDC_HDFS_Host_Chunk_Map *existing_map = get_chunk_map(hostname, locality_vector);

					if(existing_map == NULL) {
						SPDC_HDFS_Host_Chunk_Map *new_map = (SPDC_HDFS_Host_Chunk_Map*) calloc(1, sizeof(SPDC_HDFS_Host_Chunk_Map));
						memcpy(new_map, recv_map, sizeof(SPDC_HDFS_Host_Chunk_Map));
						new_map->hostname = hostname;
						new_map->chunks = chunks;
						locality_vector->push_back(new_map);
					} else {
						existing_map->num_chunks += recv_map->num_chunks;
						int* new_chunks = (int*) calloc(existing_map->num_chunks, sizeof(int));
						memcpy(new_chunks, chunks, sizeof(int)*recv_map->num_chunks);
						memcpy(new_chunks+recv_map->num_chunks, existing_map->chunks, sizeof(int)*(existing_map->num_chunks-recv_map->num_chunks));
					
						free(existing_map->chunks);
						existing_map->chunks = new_chunks;
					}

					MPI_Probe(	status.MPI_SOURCE, 
								MPI_ANY_TAG, 
								settings->comm_group, 
								&status);

					if(status.MPI_TAG == MD_LOCALITY_DIST_DONE) {
						
						MPI_Recv(	&dummy,
									1,
									MPI_INT,
									status.MPI_SOURCE,
									MD_LOCALITY_DIST_DONE,
									settings->comm_group,
									&status);

						break;
					} else {
						MPI_Recv(	&dummy,
									1,
									MPI_INT,
									status.MPI_SOURCE,
									MD_LOCALITY_DIST_MORE,
									settings->comm_group,
									&status);					
					}
				} else {
					MPI_Recv(	&dummy,
								1,
								MPI_INT,
								status.MPI_SOURCE,
								MD_LOCALITY_DIST_DONE,
								settings->comm_group,
								&status);

					break;
				}
			}
			//fprintf(stdout, "DEBUG  ---  rank: %d received finalization from %d\n", our_rank, status.MPI_SOURCE);
		}
	}

	free(recv_map);
}

void SPDC_Simulate_Chunks() {
	if(job_vector->size() > 0) {
		SPDC_HDFS_Job *j = job_vector->at(0);
		SPDC_HDFS_File_Info *working_file_info = (SPDC_HDFS_File_Info*) calloc(1, sizeof(SPDC_HDFS_File_Info));

		working_file_info->filename = j->filename;
		working_file_info->size = 1024*1024*64*10;
		working_file_info->chunk_size = 1024*1024*64;
		working_file_info->replication_factor = 3;

		file_info_vector->push_back(working_file_info);
	}

	locality_vector = new vector<SPDC_HDFS_Host_Chunk_Map*>;
	SPDC_HDFS_Host_Chunk_Map* working_locality;

	for(uint i = 0; i < file_info_vector->size(); i++) {

		//	Determine work partitioning
		uint64_t num_chunks = file_info_vector->at(i)->size / file_info_vector->at(i)->chunk_size;
		if (file_info_vector->at(i)->size % file_info_vector->at(i)->chunk_size) num_chunks++;

		uint64_t chunk_partition_size = num_chunks / settings->num_md_servers;
		chunk_partition_size = (num_chunks < (uint)settings->num_md_servers) ? 1 : chunk_partition_size;

		uint64_t start_chunk = chunk_partition_size * md_id;
		uint64_t end_chunk = start_chunk + chunk_partition_size - 1;

		if(md_id == settings->num_md_servers - 1) {
			end_chunk = num_chunks - 1;
		}

		srand(time(NULL));

		for(uint64_t j = start_chunk; j <= end_chunk; j++) {
					
			int a = rand() % settings->num_slaves;
			int b = rand() % settings->num_slaves;
			int c = rand() % settings->num_slaves;
						
			while(a == b || b == c || a == c) {
				a =  rand() % settings->num_slaves;
				b =  rand() % settings->num_slaves;
				c =  rand() % settings->num_slaves;
			}

			char **hostnames = (char**) calloc(3, sizeof(char*));

			hostnames[0] = get_hostname_from_rank(settings->slave_ranks[a], slave_hostname_vector);
			hostnames[1] = get_hostname_from_rank(settings->slave_ranks[b], slave_hostname_vector);
			hostnames[2] = get_hostname_from_rank(settings->slave_ranks[c], slave_hostname_vector);
			

			fprintf(stderr, "%d: %s %s %s\n", our_rank, hostnames[0], hostnames[1], hostnames[2]);

			for(int k = 0; k < 3; k++) {
				if(hostnames[k] == NULL) {
					continue;
				}

				//	If this is the first time we have seen this host.
				if(!contains_host(hostnames[k], locality_vector)) {
					
					//	Allocate some memory for a new locality struct
					working_locality = (SPDC_HDFS_Host_Chunk_Map*) calloc(1, sizeof(SPDC_HDFS_Host_Chunk_Map));
					
					//	Set the hostname.
					working_locality->hostname = (char*) calloc(strlen(hostnames[k]), sizeof(char));
					strcpy(working_locality->hostname, hostnames[k]);
					
					//	Allocate a spot for the chunk index and set it.
					working_locality->num_chunks = 1;
					working_locality->chunks = (int*) calloc(1, sizeof(int));
					working_locality->chunks[0] = j;

					locality_vector->push_back(working_locality);
				} else {						
					//	Get the right locality infor struct
					working_locality = get_chunk_map(hostnames[k], locality_vector);

					if(working_locality != NULL) {
						//sprintf(msg, "Localty info looks like:\t%s %d", hostnames[k], working_locality->num_chunks);
						//SPDC_Debug_Message(msg);							

						//	Make a new chunk array (since we need to dynamically resize)
						int* new_chunks = (int*) calloc(working_locality->num_chunks+1, sizeof(int));
						memcpy(new_chunks, working_locality->chunks, sizeof(int)*working_locality->num_chunks);

						//	Set the last chunk
						new_chunks[working_locality->num_chunks] = j;
						working_locality->num_chunks++;


						free(working_locality->chunks);

						//	Set the new one
						working_locality->chunks = new_chunks;
					}
				}
			}
		}
	}
}

void SPDC_Build_Locality_Map() {
	file_info_vector = new vector<SPDC_HDFS_File_Info*>;
	SPDC_HDFS_File_Info *working_file_info;
	char msg[200];

	if(sim_flag) {
		SPDC_Simulate_Chunks();
		return;
	} 

	hdfsFS fileSystem = hdfsConnect(DEFAULT_FILE_SYSTEM, 0);

	if(fileSystem == NULL) {
		sprintf(msg, "Could not connect to HDFS");
		SPDC_Debug_Message(msg);
		return;
	}

	//	First we look through all our jobs for unique files.
	for(uint i = 0; i < job_vector->size(); i++) {
		if(!contains_file_info(job_vector->at(i)->filename, file_info_vector)) {
			
			hdfsFileInfo *fileInfo;

			fileInfo = hdfsGetPathInfo(fileSystem, job_vector->at(i)->filename);

			if(fileInfo != NULL) {
				working_file_info = (SPDC_HDFS_File_Info*) calloc(1, sizeof(SPDC_HDFS_File_Info));
				working_file_info->filename = job_vector->at(i)->filename;
				working_file_info->size = fileInfo->mSize;
				working_file_info->chunk_size = fileInfo->mBlockSize;
				working_file_info->replication_factor = fileInfo->mReplication;

				file_info_vector->push_back(working_file_info);
			
				hdfsFreeFileInfo(fileInfo, 1);
			}
		}
	}	

	//	Build hostname -> chunk map for files within our offsets.
	if(file_info_vector->size() <= 0) {
		//	Do we error here?
		sprintf(msg, "No files found");
		SPDC_Debug_Message(msg);
		hdfsDisconnect(fileSystem);
		return;
	}

	locality_vector = new vector<SPDC_HDFS_Host_Chunk_Map*>;
	SPDC_HDFS_Host_Chunk_Map* working_locality;

	for(uint i = 0; i < file_info_vector->size(); i++) {

		//	Determine work partitioning
		uint64_t num_chunks = file_info_vector->at(i)->size / file_info_vector->at(i)->chunk_size;
		if (file_info_vector->at(i)->size % file_info_vector->at(i)->chunk_size) num_chunks++;


		uint64_t chunk_partition_size = num_chunks / settings->num_md_servers;
		chunk_partition_size = (num_chunks < (uint)settings->num_md_servers) ? 1 : chunk_partition_size;

		uint64_t start_chunk = chunk_partition_size * md_id;
		uint64_t end_chunk = start_chunk + chunk_partition_size - 1;

		if(md_id == settings->num_md_servers - 1) {

			end_chunk = num_chunks - 1;
		}

		for(uint64_t j = start_chunk; j <= end_chunk; j++) {
			
			char*** hosts = hdfsGetHosts(	fileSystem, 
											file_info_vector->at(i)->filename, 
											j * file_info_vector->at(i)->chunk_size, 
											1);
			if(hosts) {
				int k= 0;
				while(hosts[k]) {
					int f = 0;
					while(hosts[k][f]) {
						//	If this is the first time we have seen this host.
						if(!contains_host(hosts[k][f], locality_vector)) {
							
							//	Allocate some memory for a new locality struct
							working_locality = (SPDC_HDFS_Host_Chunk_Map*) calloc(1, sizeof(SPDC_HDFS_Host_Chunk_Map));
							
							//	Set the hostname.
							working_locality->hostname = (char*) calloc(strlen(hosts[k][f]), sizeof(char));
							strcpy(working_locality->hostname, hosts[k][f]);
							
							//	Allocate a spot for the chunk index and set it.
							working_locality->num_chunks = 1;
							working_locality->chunks = (int*) calloc(1, sizeof(int));
							working_locality->chunks[0] = j;

							locality_vector->push_back(working_locality);
						} else {						
							//	Get the right locality infor struct
							working_locality = get_chunk_map(hosts[k][f], locality_vector);

							if(working_locality != NULL) {
								//sprintf(msg, "Localty info looks like:\t%s %d", hosts[k][f], working_locality->num_chunks);
								//SPDC_Debug_Message(msg);							

								//	Make a new chunk array (since we need to dynamically resize)
								int* new_chunks = (int*) calloc(working_locality->num_chunks+1, sizeof(int));
								memcpy(new_chunks, working_locality->chunks, sizeof(int)*working_locality->num_chunks);

								//	Set the last chunk
								new_chunks[working_locality->num_chunks] = j;
								working_locality->num_chunks++;


								free(working_locality->chunks);

								//	Set the new one
								working_locality->chunks = new_chunks;
							}


						}

						++f;
					}
					++k;
				}
				hdfsFreeHosts(hosts);
			}
		}		
	}

	hdfsDisconnect(fileSystem);
}

void SPDC_Begin_Debug_Sequence() {
	int dummy;
	
	if(settings->debug_mode && !debug_sequence_initialized) {
		MPI_Send(	&dummy,
					1,
					MPI_INT,
					settings->debug_rank,
					DEBUG_SEQUENCE_INIT,
					settings->comm_group);

		debug_sequence_initialized = 1;
	}
}

void SPDC_End_Debug_Sequence() {
	int dummy;
	
	if(settings->debug_mode && debug_sequence_initialized) {
		MPI_Send(	&dummy,
					1,
					MPI_INT,
					settings->debug_rank,
					DEBUG_SEQ_FINALIZE,
					settings->comm_group);

		debug_sequence_initialized = 0;
	}
}

void SPDC_Send_Debug_Sequence_Message(char* msg) {
	int message_length;
	
	if(settings->debug_mode && debug_sequence_initialized) {
		message_length = strlen(msg);

		MPI_Send(	&message_length,
					1,
					MPI_INT,
					settings->debug_rank,
					DEBUG_SEQ_MSG_LEN,
					settings->comm_group);

		MPI_Send(	msg,
					strlen(msg),
					MPI_CHAR,
					settings->debug_rank,
					DEBUG_SEQ_MSG,
					settings->comm_group);
	}
}

void SPDC_Kill_Debug_Server() {
	int dummy;

	if(settings->debug_mode) {
		MPI_Send(	&dummy,
					1,
					MPI_INT,
					settings->debug_rank,
					DEBUG_KILL_SERVER,
					settings->comm_group);
	}
}

void SPDC_Debug_Message(char* msg) {
	if(settings->debug_mode) {
		int len = strlen(msg);

		MPI_Send(	&len,
					1,
					MPI_INT,
					settings->debug_rank,
					DEBUG_MESSAGE_INIT,
					settings->comm_group);
		
		MPI_Send(	msg,
					strlen(msg),
					MPI_CHAR,
					settings->debug_rank,
					DEBUG_MESSAGE,
					settings->comm_group);
	}
}

void SPDC_Debug_Server_Init() {
	debug_log = stdout;
	SPDC_Debug_Server();
}

void SPDC_Debug_Server() {
	MPI_Status status;
	int dummy;
	int num_done = 0;

	while(num_done < settings->nthreads - 1) {
		MPI_Probe(	MPI_ANY_SOURCE, 
					MPI_ANY_TAG, 
					settings->comm_group, 
					&status);

		if(status.MPI_TAG == DEBUG_MESSAGE_INIT) {
			int message_length;

			MPI_Recv(	&message_length,
						1,
						MPI_INT,
						status.MPI_SOURCE,
						DEBUG_MESSAGE_INIT,
						settings->comm_group,
						&status);

			char* recv_buf = (char*) calloc(message_length, sizeof(char));

			MPI_Recv(	recv_buf,
						message_length,
						MPI_CHAR,
						status.MPI_SOURCE,
						DEBUG_MESSAGE,
						settings->comm_group,
						&status);

			//	Should tokenize and print line by line, but that can be done later

			fprintf(debug_log, "%d:  ---  DEBUG:\t%s\n", status.MPI_SOURCE, recv_buf);

			free(recv_buf);

		} else if(status.MPI_TAG == DEBUG_SEQUENCE_INIT) {
			MPI_Recv(	&dummy,
						1,
						MPI_INT,
						status.MPI_SOURCE,
						DEBUG_SEQUENCE_INIT,
						settings->comm_group,
						&status);

			while(1) {
				int message_length;

				MPI_Probe(	status.MPI_SOURCE,
							MPI_ANY_TAG,
							settings->comm_group,
							&status);

				if(status.MPI_TAG == DEBUG_SEQ_MSG_LEN) {
					MPI_Recv(	&message_length,
								1,
								MPI_INT,
								status.MPI_SOURCE,
								DEBUG_SEQ_MSG_LEN,
								settings->comm_group,
								&status);

					char* recv_buf = (char*) calloc(message_length, sizeof(char));

					MPI_Recv(	recv_buf,
								message_length,
								MPI_CHAR,
								status.MPI_SOURCE,
								DEBUG_SEQ_MSG,
								settings->comm_group,
								&status);

					fprintf(debug_log, "%d:  ---  DEBUG:\t%s\n", status.MPI_SOURCE, recv_buf);

					free(recv_buf);
				} else {
					MPI_Recv(	&dummy,
								1,
								MPI_INT,
								status.MPI_SOURCE,
								DEBUG_SEQ_FINALIZE,
								settings->comm_group,
								&status);

					break;
				}
			}
		} else if (status.MPI_TAG == DEBUG_FINALIZATION) {
			MPI_Recv(	&dummy,
						1,
						MPI_INT,
						status.MPI_SOURCE,
						DEBUG_FINALIZATION,
						settings->comm_group,
						&status);

			num_done++;
		} else {
			MPI_Recv(	&dummy,	
						1,
						MPI_INT,
						status.MPI_SOURCE,
						DEBUG_KILL_SERVER,
						settings->comm_group,
						&status);

			break;
		}
	}
}

void SPDC_Debug_Print_Jobs() {
	SPDC_Begin_Debug_Sequence();
	char msg[200];
	
	sprintf(msg, "Job vector:");
	SPDC_Send_Debug_Sequence_Message(msg);


	for(uint i = 0; i < job_vector->size(); i++) {
		SPDC_HDFS_Job *j = job_vector->at(i);
		sprintf(msg, "\t%d: %s %lu %lu [", j->id, j->filename, j->start_offset, j->length);

		for(int k = 0; k < j->num_included_chunks; k++) {
			sprintf(msg+strlen(msg), "%d ", j->included_chunks[k]);
		}

		sprintf(msg+strlen(msg)-1, "]");

		SPDC_Send_Debug_Sequence_Message(msg);
	}

	sprintf(msg, "Chunks:");
	SPDC_Send_Debug_Sequence_Message(msg);

	sprintf(msg, "\t[");

	if (slave_info != NULL && slave_info->num_chunks > 0) {
		for(int i = 0; i < slave_info->num_chunks; i++) {
			sprintf(msg+strlen(msg), "%d ", slave_info->chunks[i]);
		}
		sprintf(msg+strlen(msg)-1, "]");
	} else {
		sprintf(msg, "\tNo chunks found");
	}

	SPDC_Send_Debug_Sequence_Message(msg);
	SPDC_End_Debug_Sequence();
}

void SPDC_Update_Jobs() {
	int flag = 1;
	MPI_Status status;
	
	for(int i = 0; i < settings->num_md_servers; i++) {
		int this_md = settings->md_ranks[i];

		while(flag) {
			MPI_Iprobe(	this_md,
						SLAVE_JOB_UPDATE,
						settings->comm_group,
						&flag,
						&status);

			if(flag) {
				int num_update;
				MPI_Recv(	&num_update,
							1,
							MPI_INT,
							this_md,
							SLAVE_JOB_UPDATE,
							settings->comm_group,
							&status);

				if(num_update > 0) {
					int* updated_jobs = (int*) calloc(num_update, sizeof(int));

					MPI_Recv(	updated_jobs,
								num_update,
								MPI_INT,
								this_md,
								SLAVE_SEND_UPDATED_JOBS,
								settings->comm_group,
								&status);	



					for(int i = 0; i < num_update; i++) {
						for(uint j = 0; j < job_vector->size(); j++) {
							if(job_vector->at(j)->id == updated_jobs[i]) {
								job_vector->at(j)->status = ALLOCATED;
							}
						}
					}

					free(updated_jobs);		
				}
			}
		}
	}

}

SPDC_HDFS_Job* SPDC_Get_Next_Job() {
	SPDC_HDFS_Job* r = NULL;
	MPI_Status status;
	int id;
	int depth = 0;
	//struct timeval tv;

	while(1) {
		if(num_jobs == 0) {
			num_jobs = job_vector->size();
		}

		if(job_vector->size() >= 1) {
			SPDC_Update_Jobs();

			r = job_vector->at(0);
			done_jobs->push_back(r);
			job_vector->erase(job_vector->begin());

			unsigned int index = r->id / (num_jobs / settings->num_md_servers);
			int this_md;


			if(index >= 0 && index < sorted_md_ranks->size()) {
				this_md = sorted_md_ranks->at(index);				
			} else {
				continue;
			}

			if(r->status == UN_ALLOCATED) {
				id = r->id;

				//gettimeofday(&tv, NULL);
				//double start_time = (tv.tv_sec) * 1000 + (tv.tv_usec) / 1000 ;
				
				MPI_Send(	&id,
							1,
							MPI_INT,
							this_md,
							IS_JOB_AVAILABLE,
							settings->comm_group);

				MPI_Recv(	&id,
							1,
							MPI_INT,
							this_md,
							JOB_REQUEST_RESPONSE,
							settings->comm_group,
							&status);

				//gettimeofday(&tv, NULL);
				//double end_time = (tv.tv_sec) * 1000 + (tv.tv_usec) / 1000 ;
				
				//char msg[200];
				//sprintf(msg, "\t\t*****Took %f for Response", (end_time - start_time));
				//SPDC_Debug_Message(msg);

				if(id == 1) {
					//char msg[200];
					//sprintf(msg, "\tGet job depth = %d", depth);
					//SPDC_Debug_Message(msg);
				
					char msg[200];
					sprintf(msg, "Got Job %d from md %d", r->id, this_md);
					SPDC_Send_Debug_Sequence_Message(msg);
					break;
				} else depth++;
			}
		} else break;
	}

	return r;
}

void SPDC_Finalize_Slave() {
	int dummy;
	MPI_Send(	&dummy,
				1,
				MPI_INT,
				md_rank,
				SLAVE_FINALIZATION,
				settings->comm_group);

	if(settings->debug_mode) {
		MPI_Send(	&dummy,
					1,
					MPI_INT,
					settings->debug_rank,
					DEBUG_FINALIZATION,
					settings->comm_group);
	}

	fprintf(stderr, "Slave %d finalized\n", our_rank);
}

void SPDC_Send_Debug_Finalization() {
	int dummy;
	if(settings->debug_rank) {
		MPI_Send(	&dummy,
					1,
					MPI_INT,
					settings->debug_rank,
					DEBUG_FINALIZATION,
					settings->comm_group);
	}	
}

bool SPDC_Compare_Job(const SPDC_HDFS_Job *a, const SPDC_HDFS_Job *b) {
	int a_num_similar, b_num_similar;
	a_num_similar = num_similar(a->included_chunks, a->num_included_chunks, slave_info->chunks, slave_info->num_chunks);
	b_num_similar = num_similar(b->included_chunks, b->num_included_chunks, slave_info->chunks, slave_info->num_chunks);

	return a_num_similar > b_num_similar;
}

void SPDC_Print_Local_Jobs() {
	char msg[200];

	sprintf(msg, "\tChunks");
	for(uint i = 0; i < slave_info->num_chunks; i++) {
		sprintf(msg+strlen(msg), "%d ", slave_info->chunks[i]);	
	} 

	sprintf(msg+strlen(msg), "\n");
	SPDC_Debug_Message(msg);
}

#endif

/*
 *	dlspdc.cpp
 *	Author: Andrew Foran
 *	
 *	Contents: All core interface functions.
 *	Conventions: Underscore not camelcase!!!!!!!!!
 *	
 *	TODO:	Need to add multiple files for locality_vector.
 *			Idea:	Have one vec per file with unique mas structs
 					per file
 *
 *
 */

#ifndef DLSPDC_CPP
#define DLSPDC_CPP

#include "dlspdc.h"
#include "spdc_util.cpp"


using namespace std;
vector<SPDC_HDFS_Job*> *job_vector;
vector<SPDC_HDFS_File_Info*> *file_info_vector;
vector<SPDC_HDFS_Host_Chunk_Map*> *locality_vector;
vector<SPDC_Hostname_Rank*> *slave_hostname_vector;

/*
 *	The rank of the process that called init.  We can just
 *	Store it here for later use, simplifying some things.
 */
int our_rank;
int md_id;
int sl_id;
int md_rank;

int* slaves;

/*
 *	Debug Flag 0 by Default
 */
 int debug_flag = 0;

/*
 *	Settings used to store number of threads, metadata server ranks
 *	master rank, comm group ... (Anything else we may need here)
 */
SPDC_Settings* settings;

/*
 *	Everyone calls init AFTER MPI is initialized.  Init then initializes
 *	each process according to the settings, eg: slaves get SPDC_Slave_init etc.
 *	Once all processes have been initialized, control is returned to the user.
 */
int SPDC_Init(SPDC_Settings* set, int caller_rank, int debug_mode) {

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
		our_rank = caller_rank;
		debug_flag = debug_mode;
	}

	int r;

	if(our_rank == settings->master_rank) {
		//	Initialize master rank
		r = SPDC_Master_Init();
	} else {
		int i, done = 0;

		//	Check if we are a metadata server
		for(i = 0; i < settings->num_md_servers; i++) {
			if(our_rank == settings->md_ranks[i]) {
				//	Initialize md server
				r = SPDC_Init_MD_Server();
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

	SPDC_Build_Initial_Jobs();
	SPDC_Build_Locality_Map();

	fprintf(stdout, "DEBUG  ---  rank: %d done building\n", our_rank);

	SPDC_Distribute_Locality_Map();

	fprintf(stdout, "DEBUG  ---  rank: %d done with distro\n", our_rank);

	SPDC_Receive_Locality_Map();

	fprintf(stdout, "DEBUG  ---  rank: %d done receiving other locality maps\n", our_rank);

	SPDC_Receive_Slave_Checkins();

	if(locality_vector->size() > 0 && our_rank == 1) {
		fprintf(stdout, "Rank: %d printing locality info\n", our_rank);
		print_locality_map(locality_vector);
	}

	return 0;
}

/*		**		UNTESTED		**		*/
int SPDC_Init_Slave() {
	//	Sort slave ranks and get our position
	vector<int> sorted_ranks;
	sorted_ranks.assign(settings->slave_ranks, settings->slave_ranks + settings->num_slaves);
	sort(sorted_ranks.begin(), sorted_ranks.end());
	sl_id = find(sorted_ranks.begin(), sorted_ranks.end(), our_rank) - sorted_ranks.begin();
	
	//	Use our position to find the md index we aere asosciated with
	md_id = sl_id % settings->num_md_servers;

	//	Sort md servers and get our assosciated md rank
	vector<int> sorted_md_ranks;
	sorted_md_ranks.assign(settings->md_ranks, settings->md_ranks + settings->num_md_servers);
	sort(sorted_md_ranks.begin(), sorted_md_ranks.end());

	md_rank = sorted_md_ranks.at(md_id);

	//	Send our hostname to owner md
	char* hostname = (char*) calloc(MAX_HOSTNAME_SIZE, sizeof(char));
	gethostname(hostname, MAX_HOSTNAME_SIZE);

	MPI_Send(	hostname,
				strlen(hostname),
				MPI_CHAR,
				md_rank,
				SLAVE_CHECK_IN,
				settings->comm_group);

	int dummy;
	MPI_Status status;

	MPI_Recv(	&dummy,
				1,
				MPI_INT,
				md_rank,
				SLAVE_CHECK_IN_RESP,
				settings->comm_group,
				&status);

	return 0;
}
/*		**		UNTESTED		**		*/


/*		**		UNTESTED		**		*/
void SPDC_Receive_Slave_Checkins() {
	int num_slaves = settings->num_slaves / settings->num_md_servers;

	//	If this is the last md and there is an uneven distribution we'll take it
	if(	md_id == settings->num_md_servers - 1 && 
		settings->num_slaves % settings->num_md_servers) num_slaves++;

	//	Sort slaves
	vector<int> sorted_slaves;
	sorted_slaves.assign(settings->slave_ranks, settings->slave_ranks + settings->num_slaves);
	sort(sorted_slaves.begin(), sorted_slaves.end());

	//	Calculate start and allocate mem for slaves array
	int start = md_id * num_slaves;
	slaves = (int*) calloc(num_slaves, sizeof(int));
	
	//	Copy into slaves array
	memcpy(slaves, sorted_slaves.data()+start, sizeof(int)*num_slaves);

	slave_hostname_vector = new vector<SPDC_Hostname_Rank*>;

	int num_done = 0;
	char recv_hostname[MAX_HOSTNAME_SIZE];
	MPI_Status status;
	while (num_done < num_slaves) {
		SPDC_Hostname_Rank* new_hnr;

		//	Receive a hostname
		MPI_Recv(	recv_hostname,
					MAX_HOSTNAME_SIZE,
					MPI_CHAR,
					MPI_ANY_SOURCE,
					SLAVE_CHECK_IN,
					settings->comm_group,
					&status);

		//	Allocate memory for struct and name
		new_hnr = (SPDC_Hostname_Rank*) calloc(1, sizeof(SPDC_Hostname_Rank));
		new_hnr->hostname = (char*) calloc(strlen(recv_hostname), sizeof(char));

		//	Copy hostname and rank
		memcpy(new_hnr, recv_hostname, strlen(recv_hostname)*sizeof(char));
		new_hnr->rank = status.MPI_SOURCE;

		slave_hostname_vector->push_back(new_hnr);

		MPI_Send(	&num_done,
					1,
					MPI_INT,
					status.MPI_SOURCE,
					SLAVE_CHECK_IN_RESP,
					settings->comm_group);

		num_done++;
	}
}
/*		**		UNTESTED		**		*/

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

int SPDC_MD_Server() {
	return 0;
}

void SPDC_Debug_Print_Jobs() {
	fprintf(stdout, "MDS: %d jobs:\n", our_rank);

	for(uint i = 0; i < job_vector->size(); i++) {
		print_job(job_vector->at(i));
	}
}

void SPDC_Distribute_Locality_Map() {
	SPDC_HDFS_Host_Chunk_Map* temp_map;

	for(uint i = 0; i < locality_vector->size(); i++) {
		temp_map = locality_vector->at(i);
		fprintf(stdout, "DEBUG  ---  rank: %d distributing li# %d\n", our_rank, i);
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
			}
		}
	}

	fprintf(stdout, "DEBUG  ---  rank: %d sending build finalizations\n", our_rank);
	for(int j = 0; j < settings->num_md_servers; j++) {
		if(settings->md_ranks[j] != our_rank) {
			MPI_Send(	temp_map,
						sizeof(SPDC_HDFS_Host_Chunk_Map),
						MPI_BYTE,
						settings->md_ranks[j],
						MD_LOCALITY_DIST_DONE,
						settings->comm_group);
		}
	}
}

void SPDC_Receive_Locality_Map() {
	MPI_Status status;
	SPDC_HDFS_Host_Chunk_Map *recv_map = (SPDC_HDFS_Host_Chunk_Map*) calloc(1, sizeof(SPDC_HDFS_Host_Chunk_Map));

	int num_done = 0;

	while(num_done < settings->num_md_servers-1) {

		MPI_Recv(	recv_map,
					sizeof(SPDC_HDFS_Host_Chunk_Map),
					MPI_BYTE,
					MPI_ANY_SOURCE,
					MPI_ANY_TAG,
					settings->comm_group,
					&status);

		if(status.MPI_TAG == MD_LOCALITY_DISTRIBUTION) {

			SPDC_HDFS_Host_Chunk_Map *new_map = (SPDC_HDFS_Host_Chunk_Map*) calloc(1, sizeof(SPDC_HDFS_Host_Chunk_Map));
			memcpy(new_map, recv_map, sizeof(SPDC_HDFS_Host_Chunk_Map));

			new_map->hostname = (char*) calloc(MAX_HOSTNAME_SIZE, sizeof(char));
			new_map->chunks = (int*) calloc(new_map->num_chunks, sizeof(int));

			MPI_Recv(	new_map->hostname,
						MAX_HOSTNAME_SIZE,
						MPI_CHAR,
						status.MPI_SOURCE,
						MD_LOCALITY_DIST_HOSTNAME,
						settings->comm_group,
						&status);

			MPI_Recv(	new_map->chunks,
						new_map->num_chunks,
						MPI_INT,
						status.MPI_SOURCE,
						MD_LOCALITY_DIST_CHUNKS,
						settings->comm_group,
						&status);

			locality_vector->push_back(new_map);

		} else {
			fprintf(stdout, "DEBUG  ---  rank: %d received finalization # %d\n", our_rank, num_done+1);

			num_done++;
		}
	}

	free(recv_map);
}

void SPDC_Build_Locality_Map() {
	file_info_vector = new vector<SPDC_HDFS_File_Info*>;
	SPDC_HDFS_File_Info *working_file_info;

	hdfsFS fileSystem = hdfsConnect(DEFAULT_FILE_SYSTEM, 0);

	if(fileSystem == NULL) {
		fprintf(stderr, "Could not connect to HDFS\n");
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

	locality_vector = new vector<SPDC_HDFS_Host_Chunk_Map*>;

	//	Build hostname -> chunk map for files within our offsets.
	if(file_info_vector->size() <= 0) {
		//	Do we error here?
		fprintf(stderr, "No files found\n");
		return;
	}

	locality_vector = new vector<SPDC_HDFS_Host_Chunk_Map*>;
	SPDC_HDFS_Host_Chunk_Map* working_locality;

	for(uint i = 0; i < file_info_vector->size(); i++) {

		//	Determine work partitioning
		uint64_t num_chunks = file_info_vector->at(i)->size / file_info_vector->at(i)->chunk_size;
		if (file_info_vector->at(i)->size % file_info_vector->at(i)->chunk_size) num_chunks++;


		uint64_t chunk_partition_size = num_chunks / settings->num_md_servers;
		chunk_partition_size = (num_chunks < settings->num_md_servers) ? 1 : chunk_partition_size;

		uint64_t start_chunk = chunk_partition_size * md_id;
		uint64_t end_chunk = start_chunk + chunk_partition_size - 1;

		if(md_id == settings->num_md_servers - 1) {

			end_chunk = num_chunks - 1;
		}


		for(uint64_t j = start_chunk; j <= end_chunk; j++) {
			char*** hosts = hdfsGetHosts(	fileSystem, 
											file_info_vector->at(i)->filename, 
											j * file_info_vector->at(i)->chunk_size, 
											file_info_vector->at(i)->chunk_size);

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

							//	Make a new chunk array (since we need to dynamically resize)
							int* new_chunks = (int*) calloc(working_locality->num_chunks+1, sizeof(int));
							memcpy(new_chunks, working_locality->chunks, sizeof(int)*working_locality->num_chunks);

							//	Set the last chunk
							new_chunks[working_locality->num_chunks+1] = j;
							working_locality->num_chunks++;

							//	Free the old chunk array
							free(working_locality->chunks);

							//	Set the new one
							working_locality->chunks = new_chunks;
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

#endif

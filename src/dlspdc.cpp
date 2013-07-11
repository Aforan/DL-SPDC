/*
 *	dlspdc.cpp
 *	Author: Andrew Foran
 *	
 *	Contents: All core interface functions.
 *	Conventions: Underscore not camelcase!!!!!!!!!
 */

using namespace std;
vector<SPDC_HDFS_Job*> *job_vector;


/*
 *	The rank of the process that called init.  We can just
 *	Store it here for later use, simplifying some things.
 */
int our_rank;

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
			SPDC_Abort(INVALID_SETTINGS_ERR)
		}

		settings = set;
		our_rank = caller_rank;
		debug_flag = debug_mode;
	}

	int r;

	if(our_rank == settings->master_rank) {
		//	Initialize master rank
		r = SPDC_Master_Init();
	} else if (our_rank == primary_md_rank) {
		//	Initialize primary metadata server
		r = SPDC_Primary_MD_Init();
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
			char[200] err_msg;
			sprintf(err_msg, "Rank: %d could not be identified", our_rank);
			SPDC_Aborts(UNKNOWN_ERR, err_msg);
		}
	}

	return r;
}

int SPDC_Register_HDFS_Job(SPDC_HDFS_Job* job) {
	MPI_Status status;
	int rc;

	/*
	 *	Send the job registration to the primary metadata server,
	 *  wait for the response and return accordingly (-1 fail, 0 success)
	 *	TODO: Figure out how to send custom data types ... 
	 *	NEED TO ASK: if we can just send as byte and cast later ... 
	 *	need to see how this will work, it probably will send the struct
	 *	but what about the filename ... 
	 *	Could statically allocate a filename string, but that kind of sucks
	 */

	if(job != NULL) {
		MPI_Send(	job, 
					sizeof(SPDC_HDFS_Job), 
					MPI_BYTE, 
					settings->primary_md_rank, 
					REGISTER_JOB, 
					settings->comm_group);

		MPI_Recv(	&rc, 
					1, 
					MPI_INT, 
					settings->primary_md_rank, 
					MPI_ANY_TAG, 
					settings->comm_group, 
					&status);

		rc = (status.MPI_TAG == REGISTER_JOB_SUCCESS) ? 0 : -1;
	} else {
		rc = -1;
	}

	return rc;
}

int SPDC_Master_Init() {

}

int SPDC_Primary_MD_Init() {
	/*
	 *	Begin Listening for jobs from Master.  We continue Listening
	 *	until we receive a JOB_REGISTRATION_FINISHED tag.
	 */

	job_vector = new vector<SPDC_HDFS_Job*>;

	MPI_Status status;
	SPDC_HDFS_Job *recv_job;
	int cur_job_id = 0;

	while(1) {
		//	Need to see if this will work as stands ... may need to have
		//	a void ptr and cast to job type after, we shall see.
		//	Is this job dynamically allocated, or is it local to this function..
		MPI_Recv(	recv_job, 
					sizeof(SPDC_HDFS_Job), 
					MPI_BYTE, 
					settings->master_rank, 
					MPI_ANY_TAG, 
					settings->comm_group, 
					&status);

		if(status.MPI_TAG == REGISTER_JOB) {
			recv_job->id = cur_job_id;
			job_vector.push_back(recv_job);
			cur_job_id++;
		} else if (status.MPI_TAG == JOB_REGISTRATION_FINISHED) {
			break;
		} 
	}

	//	Distribute the jobs to all other md servers
	for(uint i = 0; i < job_vector.size(); i++) {
		SPDC_HDFS_Job* cur_job = job_vector.at(i);

		for(int j = 0; j < settings->num_md_servers; j++) {
			if(settigs->md_ranks[j] == our_rank) continue;

			int dummy;

			MPI_Send(	cur_job,
						sizeof(SPDC_HDFS_Job),
						MPI_BYTE,
						settings->md_ranks[j],
						INITIAL_JOB_DISTRIBUTION,
						settings->comm_group);

			MPI_Recv(	&dummy, 
						1, 
						MPI_INT, 
						settings->md_ranks[j], 
						INITIAL_JOB_CONFIRMATION, 
						settings->comm_group, 
						&status);
		}
	}
}

int SPDC_Init_MD_Server() {

}

int SPDC_Init_Slave() {

}
#include "dlspdc.cpp"

const char* const_file = "/tmp/drosoph.nt";

#define READ_TASK 0

int main(int argc, char** argv) {
	int rank, nthreads;

	fprintf(stdout, "Initilizing MPI\n");
	
	MPI_Init(&argc,&argv);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &nthreads);

	if(nthreads != 8) {
		fprintf(stdout, "Nthreads not 8\n");

		MPI_Finalize();
		return 0;
	}

	int* md = (int*) calloc(2, sizeof(int));
	md[0] = 1;
	md[1] = 2;

	int* sr = (int*) calloc(4, sizeof(int));
	sr[0] = 3;
	sr[1] = 4;
	sr[2] = 5;
	sr[3] = 6;

	SPDC_Settings* settings;
	settings = (SPDC_Settings*) calloc(1, sizeof(SPDC_Settings));

	settings->nthreads = nthreads;
	settings->num_md_servers = 2;
	settings->num_slaves = 4;
	settings->master_rank = 0;
	settings->md_ranks = md;
	settings->slave_ranks = sr;
	settings->comm_group = MPI_COMM_WORLD;
	settings->debug_mode = 1;
	settings->debug_rank = 7;

	SPDC_Init(settings, rank);

	if(rank == 0) {
		//fprintf(stderr, "Registering some jobs in master\n");
		char msg[200];
		sprintf(msg, "Registering some jobs in master");

		SPDC_Debug_Message(msg);

		SPDC_HDFS_Job* working_job = (SPDC_HDFS_Job*) calloc(1, sizeof(SPDC_HDFS_Job));
		char* filename = (char*) calloc(strlen(const_file), sizeof(char));
		strcpy(filename, const_file);

		for(int i = 0; i < 10; i++) {
			working_job->id = i;
			working_job->tag = READ_TASK;
			working_job->filename = filename;
			working_job->filename_length = strlen(filename);
			working_job->start_offset = 0;
			working_job->length = 1024*1024*100;

			SPDC_Register_HDFS_Job(working_job);	
		}

		SPDC_Finalize_Registration();
	} else {
		//if(rank == 1)
		//	SPDC_Debug_Print_Jobs();
	}

	fprintf(stderr, "Rank: %d done\n", rank);

	MPI_Finalize();
	return 0;
}

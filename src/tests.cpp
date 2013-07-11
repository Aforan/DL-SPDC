#include "dlspdc.cpp"

const char* const_file = "/tmp/drosoph.nt";

#define READ_TASK 0

int main(int argc, char** argv) {
	int rank, nthreads;

	fprintf(stdout, "Initilizing MPI\n");
	
	MPI_Init(&argc,&argv);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &nthreads);

	if(nthreads != 3) {
		fprintf(stdout, "Nthreads not 3\n");

		MPI_Finalize();
		return 0;
	}

	int* md = (int*) calloc(2, sizeof(int));
	md[0] = 1;
	md[1] = 2;

	SPDC_Settings* settings;
	settings = (SPDC_Settings*) calloc(1, sizeof(SPDC_Settings));

	settings->nthreads = nthreads;
	settings->num_md_servers = 2;
	settings->num_slaves = 0;
	settings->master_rank = 0;
	settings->md_ranks = md;
	settings->comm_group = MPI_COMM_WORLD;

	SPDC_Init(settings, rank, 0);

	if(rank == 0) {
		fprintf(stderr, "Registering some jobs in master\n");

		SPDC_HDFS_Job* working_job = (SPDC_HDFS_Job*) calloc(1, sizeof(SPDC_HDFS_Job));
		char* filename = (char*) calloc(strlen(const_file), sizeof(char));
		strcpy(filename, const_file);

		for(int i = 0; i < 10; i++) {
			working_job->id = i;
			working_job->tag = READ_TASK;
			working_job->filename = filename;
			working_job->filename_length = strlen(filename);
			working_job->start_offset = i*10;
			working_job->length = 10;

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

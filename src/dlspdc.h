
#define REGISTER_JOB 				0
#define REGISTER_JOB_SUCCESS 		1
#define REGISTER_JOB_FAILURE 		2
#define JOB_REGISTRATION_FINISHED	3
#define INITIAL_JOB_DISTRIBUTION	4
#define INITIAL_JOB_CONFIRMATION	5

typedef struct SPDC_Settings_Struct {
	int nthreads;
	int* md_ranks;
	int* slave_ranks;
	int num_slaves;
	int num_md_servers;
	int master_rank;
	int primary_md_rank;
	MPI_COMM comm_group;
} SPDC_Settings;

typedef struct SPDC_HDFS_Job_Struct {
	int id;
	int tag;
	char* filename;
	uint64_t start_offset;
	uint64_t length;	
} SPDC_HDFS_Job;
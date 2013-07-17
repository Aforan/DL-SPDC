#include "dlspdc.cpp"

#define READ_TASK 0
//USEAGE: $ test -nm nummd -ns numslaves -sb slavebegin -se slaveend [-d debugrank]

char* file = NULL;
int num_md, num_slaves, slave_begin, slave_end, debug_rank, debug_mode;

void validate();
void parse_args(int argc, char** argv);

int main(int argc, char** argv) {
	int rank, nthreads;

	fprintf(stdout, "Initilizing MPI\n");
	
	MPI_Init(&argc,&argv);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &nthreads);

	num_md = -1;
	num_slaves = -1;
	slave_begin = -1;
	slave_end = -1;
	debug_rank = -1;
	debug_mode = -1;

	parse_args(argc, argv);
	validate();

	int* md = (int*) calloc(num_md, sizeof(int));

	for(int i = 0; i < num_md; i++) {
		md[i] = 1+i;
	}

	int* sr = (int*) calloc(num_slaves, sizeof(int));
	for(int i = 0; i < num_slaves; i++) {
		sr[i] = slave_begin + i;
	}

	SPDC_Settings* settings;
	settings = (SPDC_Settings*) calloc(1, sizeof(SPDC_Settings));

	settings->nthreads = nthreads;
	settings->num_md_servers = num_md;
	settings->num_slaves = num_slaves;
	settings->master_rank = 0;
	settings->md_ranks = md;
	settings->slave_ranks = sr;
	settings->comm_group = MPI_COMM_WORLD;
	settings->debug_mode = debug_mode;
	settings->debug_rank = debug_rank;

	SPDC_Init(settings, rank);

	if(rank == 0) {
		char msg[200];
		sprintf(msg, "Registering some jobs in master");
		SPDC_Debug_Message(msg);

		SPDC_HDFS_Job* working_job = (SPDC_HDFS_Job*) calloc(1, sizeof(SPDC_HDFS_Job));
		char* filename = (char*) calloc(strlen(file), sizeof(char));
		strcpy(filename, file);

		for(uint64_t i = 0; i < 114; i++) {
			working_job->id = i;
			working_job->tag = READ_TASK;
			working_job->filename = filename;
			working_job->filename_length = strlen(filename);
			working_job->start_offset = (i*1024*1024*64);
			working_job->length = (1024*1024*64);
			working_job->status = UN_ALLOCATED;

			SPDC_Register_HDFS_Job(working_job);	
		}

		free(working_job);

		SPDC_Finalize_Registration();
		SPDC_Send_Debug_Finalization();
	} else {
		if(rank >= slave_begin && rank <= slave_end) {
			SPDC_HDFS_Job* job;
			char msg[200];

			//SPDC_Begin_Debug_Sequence();
			sprintf(msg, "Beginning Jobs");
			SPDC_Debug_Message(msg);
			while((job = SPDC_Get_Next_Job(0)) != NULL) {	
				sprintf(msg, "\tGot next job %d", job->id);
				SPDC_Debug_Message(msg);
				
				hdfsFS file_system = hdfsConnect(DEFAULT_FILE_SYSTEM, 0);

			   	hdfsFile hdfs_file = hdfsOpenFile(file_system, job->filename, O_RDONLY, 0, 0, 0);
			   	
			   	if(hdfs_file != NULL) {
					char buf[4096];

				   	uint64_t i;

				   	for(i = 0; i < job->length; i+=4096) {
				   		hdfsPread(file_system, hdfs_file, job->start_offset+i, buf, 4096);	
				   	}
				   	
				   	//hdfsPread(file_system, hdfs_file, job->start_offset+(i-1), buf, (job->start_offset + job->length) - job->start_offset+(i-1));
				   	hdfsCloseFile(file_system, hdfs_file);
			   	}

			   	hdfsDisconnect(file_system);
			}
			sprintf(msg, "Ending jobs");
			SPDC_Debug_Message(msg);
			//SPDC_End_Debug_Sequence();

			SPDC_Finalize_Slave();
		}
	}

	//fprintf(stderr, "Rank: %d done\n", rank);

	MPI_Finalize();
	return 0;
}

//USEAGE: $ test -nm nummd -ns numslaves -sb slavebegin -se slaveend -f file [-d debugrank]
void parse_args(int argc, char** argv) {
	int i = 1;
	while(1) {
		if(i < argc) {
			if(!strcmp(argv[i], "-nm")) {
				num_md = atoi(argv[++i]);
				++i;
			} else if(!strcmp(argv[i], "-ns")) {
				num_slaves = atoi(argv[++i]);
				++i;
			} else if(!strcmp(argv[i], "-sb")) {
				slave_begin = atoi(argv[++i]);
				++i;
			} else if(!strcmp(argv[i], "-se")) {
				slave_end = atoi(argv[++i]);
				++i;
			} else if(!strcmp(argv[i], "-d")) {
				debug_rank = atoi(argv[++i]);
				debug_mode = 1;
				++i;
			} else if(!strcmp(argv[i], "-f")) {
				file = argv[++i];
				i++;
			} else {
				fprintf(stderr, "invalid useage: $ test -nm nummd -ns numslaves -sb slavebegin -se slaveend -f file [-d debugrank]\n");
				MPI_Finalize();
				exit(-1);
			}			
		} else break;
	}
}

void validate() {
	int r = (num_md != -1) &
	 (num_slaves != -1) &
	 (slave_begin != -1) &
	 (slave_end != -1) &
	 (((debug_mode == 1) & debug_rank != -1) |
	 	debug_mode != 1) &
	 (file != NULL);

	 if(!r) {
	 	fprintf(stderr, "Invalid Args\n");
	 	MPI_Finalize();
	 	exit(-1);
	 }
}

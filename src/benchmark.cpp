#include <list>
#include "mpi.h"
#include "hdfs.h"
#include <string>
#include <vector>
#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <unistd.h>
#include <sys/time.h>
#include "metric.cpp"
#include <sys/stat.h>
#include "fileCheck.c"

#ifndef SLAVE_CPP
#define SLAVE_CPP
#include "Slave.cpp"
#endif

#ifndef CHUNK_CPP
#define CHUNK_CPP
#include "chunk.cpp"
#endif

#ifndef LOCALITY_INFO_CPP
#define LOCALITY_INFO_CPP
#include "LocalityInfo.cpp"
#endif

#define SLAVE_IDLE 1
#define SCHEDULE_TASK 0
#define CONFIRM_CLOSE 3
#define CHUNK_SIZE 67108864
#define COMPUTATION_FINISHED 2

#define DATA_LOCALITY_MODE 0
#define ARBITRARY_MODE 1
#define RUNTIME_MODE 2

int rank, nthreads;
char *cpu_name;
int mode;
int numDuplications;
Metric *metrics;
uint64_t fileSize;
uint64_t chunkSize;

std::string hostFile;
std::string filename;

std::vector<Slave*> slaves;
std::vector<Chunk*> chunks;
std::vector<std::string> hosts;
std::vector<LocalityInfo*> localityInfoList;
std::vector<uint64_t> jobTimes;
std::vector<int> lastJobType;

int isDone();
void slaveInit();
void superLoop();
void slaveLoop();
int allTrue(int*);
void initChunks();
void initHDFSChunks();
int getPid(std::string);
void superFinalize(time_t, uint64_t*);
void parseArgs(int, char**);
void superInit(int, char**);
void scheduleByLocality(int);
void scheduleByArbitrary(int);
void parseHostFile(std::string);
uint64_t getFileSize(std::string);
void runtimeAwareLocalityScheduling(int);
unsigned long getHDFSFileSize(std::string filename);

int main(int argc, char** argv) {
    parseArgs(argc, argv);

	MPI_Init(&argc,&argv);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &nthreads);
	cpu_name = (char *)calloc(80,sizeof(char));
	gethostname(cpu_name,80);

    if(rank == 0) {
        superInit(argc, argv);
        superLoop();
    } else {
        slaveInit();
        slaveLoop();
    }

    MPI_Finalize();

    return 0;
}

int isDone() {
    for(unsigned int i = 0; i < chunks.size(); i++) {
        if(chunks.at(i)->processed == 0) {
            return 0;
        }
    }

    //  If there are still duplications to do, reset all chunks
    if(numDuplications > 1) {
        numDuplications--;
        for(unsigned int i = 0; i < chunks.size(); i++) {
            chunks.at(i)->processed = 0;
        }
        return 0;
    }

    return 1;
}


void superLoop() {
	MPI_Status status;
    unsigned long recvBuf[5];
    uint64_t res[4];
    memset(res, 0, sizeof(res));

    time_t start = time(NULL);
    metrics = new Metric(start, mode, nthreads, filename, fileSize, chunks.size(), chunkSize, numDuplications);

	while(!isDone()) {
        //	Receive a msg
        MPI_Recv(recvBuf, 5, MPI_UNSIGNED_LONG, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

		if(status.MPI_TAG == SLAVE_IDLE) {
            //  If the job was not completed update processed ...
            if(recvBuf[4] == 0) {
                int reScheduleIndex = localityInfoList.at(status.MPI_SOURCE - 1)->slave->curChunkIndex;
                std::cerr << "File read error, rescheduling chunk " << reScheduleIndex << std::endl;

                if(reScheduleIndex == -1) {
                    std::cerr << "Got File read error on a first chunk, ignoring" << std::endl;
                } else {
                    chunks.at(reScheduleIndex)->processed = 0;
                }
            } else {
                res[0] += recvBuf[0];
                res[1] += recvBuf[1];
                res[2] += recvBuf[2];
                res[3] += recvBuf[3];
            }

            time_t beginScheduleTime = time(NULL);

            //  If this isn't the first job for this slave, finish the job in metrics
            if(jobTimes.at(status.MPI_SOURCE - 1) != 0) {
                time_t elapsedJobTime = beginScheduleTime - jobTimes.at(status.MPI_SOURCE - 1);
                metrics->finishJob(elapsedJobTime, lastJobType.at(status.MPI_SOURCE - 1));
            }


            if(mode == DATA_LOCALITY_MODE) {
                //  Finish the Last job at the idle slave, and schedule next job
                slaves.at(status.MPI_SOURCE - 1)->finishJob();
                scheduleByLocality(status.MPI_SOURCE);
            } else if(mode == ARBITRARY_MODE) {
                slaves.at(status.MPI_SOURCE - 1)->finishJob();
                scheduleByArbitrary(status.MPI_SOURCE);
            } else if (mode == RUNTIME_MODE) {
                slaves.at(status.MPI_SOURCE - 1)->finishJob();
                runtimeAwareLocalityScheduling(status.MPI_SOURCE);
            } else {
                std::cerr << "Mode not set properly, critical error exit..." << std::endl;
                return;
            }

            //  Log the schedule time and save the time the job started (just the current time)
            time_t endScheduleTime = time(NULL);
            jobTimes.at(status.MPI_SOURCE - 1) = endScheduleTime;
            metrics->logScheduleTime(endScheduleTime - beginScheduleTime);
		}
	}

    superFinalize(start, res);
}

int allTrue(int* r) {
    for(int i = 0; r[i] != -1; i++) {
        if(r[i] == 0) return 0;
    }

    return 1;
}

void slaveLoop() {
	MPI_Status status;
    int shouldClose = 0;
    unsigned long ret[5];
    memset(ret, 0, sizeof(ret));
    ret[4] = 1;
    MPI_Send(&ret, 5, MPI_UNSIGNED_LONG, 0, SLAVE_IDLE, MPI_COMM_WORLD);
    struct timeval  tv;

    double readTime = 0;

	while(!shouldClose) {
        unsigned long offsets[2];
        memset(ret, 0, sizeof(ret));
        ret[4] = 1;
		//	Receive a msg
		MPI_Recv(offsets, 2, MPI_UNSIGNED_LONG, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
		if(status.MPI_TAG == SCHEDULE_TASK) {
            unsigned long length = offsets[1] - offsets[0];
            char in[1024];

            //MPI_File file;
            //MPI_Status fileStatus;
/*
            std::string mountedFilename;
            unsigned long fsz;

            if(mode == DATA_LOCALITY_MODE || mode == RUNTIME_MODE) {
                mountedFilename = "/tmp/AndrewHDFS" + filename;
                fsz = getHDFSFileSize(filename);

                if (offsets[0] + length > fsz) {
                    length = fsz - offsets[0];
                }
            } else {
                fsz = getFileSize(filename);

                if (offsets[0] + length > fsz) {
                    length = fsz - offsets[0];
                }

                mountedFilename = filename;
            }

            //int r = MPI_File_open(MPI_COMM_SELF, (char*)mountedFilename.c_str(), MPI_MODE_RDONLY, MPI_INFO_NULL, &file);
*/
            gettimeofday(&tv, NULL);
            double start_time = (tv.tv_sec) * 1000 + (tv.tv_usec) / 1000 ;
/*
            std::ifstream inFile;
            inFile.open((char*)mountedFilename.c_str());
*/
            if(0) {
                //  If we were unable to open file, tell master
                std::cerr << "Slave " << rank << " Unable to read file " << filename << std::endl;
                ret[4] = 0;
                MPI_Send(ret, 5, MPI_UNSIGNED_LONG, 0, SLAVE_IDLE, MPI_COMM_WORLD);
            } else {
                //  If we were able to open the file, read it.
/*
                //std::cerr << "\t\tSlave " << rank << " reading chunk " << offsets[0] / CHUNK_SIZE << std::endl;
                memset(ret, 0, sizeof(ret));
                ret[4] = 1;
                unsigned long cur = 0;
                inFile.seekg(offsets[0]);

                while(cur <= length) {
                    memset(in, '\0', sizeof(in));
                    //MPI_File_read_at(file, offsets[0] + cur, in, 1024, MPI_CHAR, &fileStatus);
                    //std::cerr << " \t\tSlave " << rank << " read 1024 Bytes, has " << length - cur << " Bytes left" << " On chunk " << offsets[0] / CHUNK_SIZE << std::endl;

                    inFile.read(in, 1024);

                    for(int i = 0; in[i] != '\0'; i++) {
                        switch(in[i]) {
                        case 'T':
                            ret[0]++;
                            break;
                        case 'A':
                            ret[1]++;
                            break;
                        case 'G':
                            ret[2]++;
                            break;
                        case 'C':
                            ret[3]++;
                            break;
                        }
                    }

                    cur += 1024;
                }
*/
                srand(time(NULL));
                double f =  + ((double)rand() / RAND_MAX) * (0.75);

                usleep((1250 + (int)(f * 1000)) * 1000);
                //std::cerr << "Slave " << rank << " read " << (1250 + (int)(f * 1000)) <<std::endl;
                //std::cerr << "Slave returning " << ret[0] << ", " << ret[1] << ", " << ret[2] << ", " << ret[3] << std::endl;
                //  Tell master we're idle
                //MPI_File_close(&file);
                //inFile.close();

                gettimeofday(&tv, NULL);
                double end_time = (tv.tv_sec) * 1000 + (tv.tv_usec) / 1000 ;
                readTime += (end_time - start_time);

                MPI_Send(ret, 5, MPI_UNSIGNED_LONG, 0, SLAVE_IDLE, MPI_COMM_WORLD);
            }
		} else {
            shouldClose = 1;
            ret[0] = (unsigned long) readTime;
            MPI_Send(ret, 5, MPI_UNSIGNED_LONG, 0, CONFIRM_CLOSE, MPI_COMM_WORLD);
		}
	}
}

void superFinalize(time_t start, uint64_t* res) {
    MPI_Status status;
    unsigned long recvBuf[5];
    int doneThreads[nthreads+1];
	memset(doneThreads, 0, sizeof(doneThreads));
	doneThreads[0] = 1;
	doneThreads[nthreads] = -1;

    while(!allTrue(doneThreads)) {
        //std::cerr << "Done processing chunks, waiting for message" << std::endl;
        MPI_Recv(recvBuf, 5, MPI_UNSIGNED_LONG, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

        if(status.MPI_TAG == CONFIRM_CLOSE) {
            //std::cerr << "Slave " << status.MPI_SOURCE << " marked as done" << std::endl;
            doneThreads[status.MPI_SOURCE] = 1;
            slaves.at(status.MPI_SOURCE - 1)->read_time = recvBuf[0];
            //for(int i = 0; doneThreads[i] != -1; i++) std::cerr << " Slave " << i << ": " << doneThreads[i];
            //std::cerr << std::endl;
        } else {
            time_t beginScheduleTime = time(NULL);
            //  If this isn't the first job for this slave, finish the job in metrics
            if(jobTimes.at(status.MPI_SOURCE - 1) != 0) {
                time_t elapsedJobTime = beginScheduleTime - jobTimes.at(status.MPI_SOURCE - 1);
                metrics->finishJob(elapsedJobTime, lastJobType.at(status.MPI_SOURCE - 1));
            }

            //  If the job was not completed update processed ...
            if(recvBuf[4] == 0) {
                int reScheduleIndex = localityInfoList.at(status.MPI_SOURCE - 1)->slave->curChunkIndex;
                std::cerr << "File read error, rescheduling chunk " << reScheduleIndex << std::endl;
                chunks.at(reScheduleIndex)->processed = 0;
            } else {
                res[0] += recvBuf[0];
                res[1] += recvBuf[1];
                res[2] += recvBuf[2];
                res[3] += recvBuf[3];
            }

            //  Tell The thread were done
            //unsigned long *l = (unsigned long*)malloc(sizeof(unsigned long) * 2);
            unsigned long  l[2];
            MPI_Send(l, 2, MPI_UNSIGNED_LONG, status.MPI_SOURCE, COMPUTATION_FINISHED, MPI_COMM_WORLD);
        }
    }

    time_t end = time(NULL);
    metrics->finish(end);

    std::cerr << "Master " << rank << " Computation finished, took " << (end-start) << " seconds" << std::endl;

    std::cerr << std::endl;

    for(unsigned int i = 0; i < slaves.size(); i++) {
        std::cerr << "Slave " << slaves.at(i)->pid << " completed " << slaves.at(i)->numDoneJobs << " jobs with read time: " << slaves.at(i)->read_time << " ms" << std::endl;
    }

    std::cerr << std::endl << "RESULTS:" << std::endl;

    for(int i = 0; i < 4; i++) {
        std::cerr << i << " : " << res[i] << std::endl;
    }

    metrics->slaves = slaves;
    metrics->dumpMetrics(std::string("out"));
}

std::string getHostName(int pid) {
    for(unsigned int i = 0; i < slaves.size(); i++) {
        if(slaves.at(i)->pid == pid) return slaves.at(i)->hostName;
    }
    return NULL;
}

int getPid(std::string hostName) {
    for(unsigned int i = 0; i < slaves.size(); i++) {
        if(!hostName.compare(slaves.at(i)->hostName)) return slaves.at(i)->pid;
    }
    return -1;
}


void scheduleByLocality(int source) {
    //  Get the slave associated with source pid
    Slave *tSlave = localityInfoList.at(source-1)->slave;
    std::vector<Chunk*> *localChunks = localityInfoList.at(source-1)->chunks;

    unsigned long offsets[2];
    int scheduled = 0;

    if(!source) {
        //  If we cant find the slave, something went wrong
        std::cerr << " Could not find the machine of host " << source << " critical error, exit..." << std::endl;
        exit(-1);
    } else {

        //  For all the local chunks,
        for(unsigned int i = 0; i < localChunks->size(); i++) {
            //  If the chunk is unprocessed, schedule it
            if (localChunks->at(i)->processed == 0) {
                offsets[0] = localChunks->at(i)->index * CHUNK_SIZE;
                offsets[1] = (localChunks->at(i)->index + 1) * CHUNK_SIZE - 1;

                MPI_Send(offsets, 2, MPI_UNSIGNED_LONG, source, SCHEDULE_TASK, MPI_COMM_WORLD);
                localChunks->at(i)->processed = 1;

                int temp = (int) localChunks->at(i)->index;

                tSlave->startJob(temp);
                scheduled = 1;
                break;
            }
        }

        //  If we're still not scheduled schedule the first chunk
        if(!scheduled) {
            //std::cerr << "No local file for " << filename <<  " for host " << tSlave->hostName;

            //  Find the first unprocesed chunk and schedule it
            for(unsigned int i = 0; i < chunks.size(); i++) {
                if(chunks.at(i)->processed == 0) {
                    offsets[0] = chunks.at(i)->index * CHUNK_SIZE;
                    offsets[1] = ((chunks.at(i)->index + 1) * CHUNK_SIZE) - 1;
                    MPI_Send(offsets, 2, MPI_UNSIGNED_LONG, source, SCHEDULE_TASK, MPI_COMM_WORLD);
                    chunks.at(i)->processed = 1;

                    int temp = (int) chunks.at(i)->index;

                    tSlave->startJob(temp);
                    std::cerr << " Scheduled chunk " << temp << std::endl;
                    break;
                }
            }

            lastJobType.at(source-1) = NON_LOCAL_JOB;
        } else {
            lastJobType.at(source-1) = LOCAL_JOB;
        }
    }
}

void runtimeAwareLocalityScheduling(int source) {
    Slave* idleSlave = slaves.at(source-1);
    std::vector<Chunk*> localIdleUnprocChunks = localityInfoList.at(source-1)->getUnprocessedChunks();

    float min = 0;
    Chunk* minChunk;
    Chunk* scheduleChunk;
    float idleWeight = (float) localIdleUnprocChunks.size() * (float) (1.0f / idleSlave->numDoneJobs);

    //  If there are no unprocessed chunks locally, just schedule the first
    //  unprocessed chunk we find, for now...
    if(localIdleUnprocChunks.size() == 0) {
        std::cerr << "No local Chunks found for slave " << source << " scheduling arbitrary." << std::endl;
        lastJobType.at(source-1) = NON_LOCAL_JOB;
        for(unsigned int i = 0; i < chunks.size(); i++) {
            if(chunks.at(i)->processed == 0) {
                scheduleChunk = chunks.at(i);
                break;
            }
        }
    } else {
        for(unsigned int i = 0; i < localIdleUnprocChunks.size(); i++) {
            float max = idleWeight;
            Chunk* curChunk = localIdleUnprocChunks.at(i);

            //  Calculate the largest weighted runtime for scheduling the current
            //  Unprocessed chunk for each slave (not including the idle slave)
            for(unsigned int j = 0; j < slaves.size(); j++) {
                //  Skip the idle slave
                if(slaves.at(j) == idleSlave) continue;

                std::vector<Chunk*> tUnprocChunks = localityInfoList.at(j)->getUnprocessedChunks();
                unsigned int tRemainingJobs = tUnprocChunks.size();

                //  Try and find the unprocessed chunk, if we find it reduce the number of remaining jobs for this slave
                if(std::find(tUnprocChunks.begin(), tUnprocChunks.end(), curChunk) != tUnprocChunks.end()) {
                    tRemainingJobs--;
                }

                //  Calculate the weight of scheduling this chunk, and update max
                float weight = (float)tRemainingJobs * (float) (1.0f / slaves.at(j)->numDoneJobs);
                max = (max < weight) ? weight : max;
            }

            //  If min has yet to be set, set the initial value, and save this chunk
            if (min == 0) {
                min = max;
                minChunk = curChunk;
            } else {
                //  If our last min is greater than the computed max weight for this chunk.
                if(min > max) {
                    //  Update min and save this chunk
                    min = max;
                    minChunk = curChunk;
                }
            }
        }

        scheduleChunk = minChunk;
        lastJobType.at(source-1) = LOCAL_JOB;
    }

    if(scheduleChunk) {
        std::cerr << "Scheduling chunk " << scheduleChunk->index << std::endl;

        scheduleChunk->processed = 1;
        idleSlave->startJob(scheduleChunk->index);

        unsigned long offsets[2];
        offsets[0] = scheduleChunk->index * CHUNK_SIZE;
        offsets[1] = (scheduleChunk->index + 1) * CHUNK_SIZE;

        MPI_Send(offsets, 2, MPI_UNSIGNED_LONG, source, SCHEDULE_TASK, MPI_COMM_WORLD);
    } else {
        //  If we did not find a chunk to schedule (which should not happen) just send start and end as 0
        //  The slave should read nothing and send another idle message to master, this way we won't lose the slave
        std::cerr << "Slave " << source << " could not find a chunk to schedule ... " << std::endl;
        unsigned long offsets[2];
        offsets[0] = 0;
        offsets[1] = 0;

        MPI_Send(offsets, 2, MPI_UNSIGNED_LONG, source, SCHEDULE_TASK, MPI_COMM_WORLD);
    }

}

void scheduleByArbitrary(int source) {
    //Chunk *tempChunk = chunks.front();
    //chunks.erase(chunks.begin());

    Chunk* tempChunk;
    for(unsigned int i = 0; i < chunks.size(); i++) {
        if(chunks.at(i)->processed == 0) {
            tempChunk = chunks.at(i);
            tempChunk->processed = 1;
            break;
        }
    }


    if(!tempChunk) {
        std::cerr << "Arbitrary scheduler cannot find unprocessed chunks, critical error, exit..." << std::endl;
        MPI_Finalize();
        exit(-1);
    }

    lastJobType.at(source-1) = NON_LOCAL_JOB;

    unsigned long offsets[2];
    offsets[0] = tempChunk->index * CHUNK_SIZE;
    offsets[1] = (tempChunk->index + 1) * CHUNK_SIZE;

    MPI_Send(offsets, 2, MPI_UNSIGNED_LONG, source, SCHEDULE_TASK, MPI_COMM_WORLD);
}

void slaveInit() {
}

void superInit(int argc, char** argv) {
    std::cerr << "Starting Master process rank: " << rank << std::endl;
    parseHostFile(hostFile);

    std::cerr << "Initializing slaves ... " << std::endl;

    //Initialize the Slaves
    for(int i = 1; i < nthreads; i++) {
        std::string hostName = hosts.at(i%hosts.size());
        Slave *tSlave = new Slave(i, hostName);
        slaves.push_back(tSlave);
        jobTimes.push_back(0);
        lastJobType.push_back(LOCAL_JOB);
    }

    std::cerr << "Initializing Locality Info ... " << std::endl;
    //  Initialize LocalityInfo
    for(int i = 1; i < nthreads; i++) {
        Slave *tSlave = slaves.at(i-1);
        LocalityInfo *tLocalityInfo = new LocalityInfo(filename, tSlave);
        localityInfoList.push_back(tLocalityInfo);
    }

    if(mode == DATA_LOCALITY_MODE || mode == RUNTIME_MODE) {
        initHDFSChunks();
    } else if (mode == ARBITRARY_MODE) {
        std::cerr << "Initializing Chunks ... " << std::endl;
        initChunks();
    } else {
        std::cerr << "Invalid mode, exit" << std::endl;
    }

    //  Print out the LocalityInfo generated
    for(unsigned int i = 0; i < localityInfoList.size(); i++) {
        LocalityInfo *tLI = localityInfoList.at(i);
        std::vector<Chunk*> *tChunks = tLI->chunks;
        std::cerr << tLI->slave->hostName << " has Chunks: " << std::endl;

        for(unsigned int j = 0; j < tChunks->size(); j++) {
            std::cerr << "\t"  << tChunks->at(j)->filename << " : " << tChunks->at(j)->index << std::endl;
        }
    }
}

void initHDFSChunks() {
    hdfsFS fileSystem = hdfsConnect(DEFAULT_FILE_SYSTEM, 0);

    if(!fileSystem) {
        fprintf(stderr, "\tCould not connect to file system\n");
        return;
    }

    //  Get the fileInfo
    hdfsFileInfo *fileInfo = hdfsGetPathInfo(fileSystem, filename.c_str());

    if(!fileInfo) {
        fprintf(stderr, "\tCould not get path info\n");
        return;
    }

    fileSize = fileInfo->mSize;
    chunkSize = fileInfo->mBlockSize;
    std::cerr << "Chunk size " << chunkSize << " filesize " << fileSize << std::endl;
    int i;

    for(i = 0; (i * chunkSize) < fileSize; i++) {
        //  Create the chunk and add it to our list of chunks
        Chunk *tempChunk = new Chunk(i, filename);
        chunks.push_back(tempChunk);

        //  Calculate the offsets
        uint64_t start = i * chunkSize;
        uint64_t end = (i + 1) * chunkSize > fileSize ? fileSize : (i + 1) * chunkSize;

        //  Get the hosts containing this chunk
        std::vector<std::string> tempHosts;
        char*** hosts = machineList(filename.c_str(), start, end);

        //  Translate the hosts to strings
        if(hosts) {
            int f = 0;
            while(hosts[f]) {
                int j = 0;
                while(hosts[f][j]) {
                    tempHosts.push_back(std::string (hosts[f][j]));
                    ++j;
                }
                ++f;
            }
        }

        //  For each best Host,
        for(unsigned int j = 0; j < tempHosts.size(); j++) {
            std::string tempHostName = tempHosts.at(j);
            int s = tempHostName.find(".");

            //  Erase ".local"
            tempHostName.erase(s, tempHostName.size());
            int pid = getPid(tempHostName);

            //  Add the Chunk to each LocalityInfo corresponding to the PID
            if(pid != -1) {
                LocalityInfo* tLI = localityInfoList.at(pid-1);
                tLI->chunks->push_back(tempChunk);
            }
        }
    }

    //  Cleanup
    hdfsDisconnect(fileSystem);
    hdfsFreeFileInfo(fileInfo, 1);
}

void initChunks() {
    fileSize = getFileSize(filename);
    chunkSize = CHUNK_SIZE;

    if(fileSize == 0) {
        std::cerr << "Could not stat " << filename << " critical error, exit" << std::endl;
        exit(-1);
    }
    std::cerr << "fileSize got : " << fileSize << std::endl;

    for(uint64_t i = 0; (i * CHUNK_SIZE) < fileSize; i++) {
        std::cerr << "\tinitting chunk " << i << std::endl;
        Chunk *tempChunk = new Chunk(i, filename);
        chunks.push_back(tempChunk);
    }

    std::cerr << "Chunks initialized " << std::endl;
}

uint64_t getFileSize(std::string filename) {
    struct stat stat_buf;
    int rc = stat(filename.c_str(), &stat_buf);
    return rc == 0 ? stat_buf.st_size : 0;
}

unsigned long getHDFSFileSize(std::string filename) {
    hdfsFS fileSystem = hdfsConnect(DEFAULT_FILE_SYSTEM, 0);

    if(!fileSystem) {
        fprintf(stderr, "\tCould not connect to file system\n");
        return 0;
    }

    //  Get the fileInfo
    hdfsFileInfo *fileInfo = hdfsGetPathInfo(fileSystem, filename.c_str());

    if(!fileInfo) {
        fprintf(stderr, "\tCould not get path info\n");
        return 0;
    }

    tOffset fileSize = fileInfo->mSize;

    hdfsDisconnect(fileSystem);
    hdfsFreeFileInfo(fileInfo, 1);

    return (unsigned long)fileSize;
}

void parseHostFile(std::string hostFile) {
    std::cerr << "Parsing host file" << std::endl;
    FILE* hfh = fopen((char*)hostFile.c_str(), "r");

    if(!hfh) {
        std::cerr << "Could not open hostfile, exit..." << std::endl;
        exit(-1);
    }

    char buf[512];

    while((fgets(buf, 512, hfh)) != NULL) {
        if(buf[strlen(buf)-1] == '\n') {
            buf[strlen(buf)-1] = '\0';
        }

        std::string tempHost (buf);
        hosts.push_back(tempHost);
    }

    fclose(hfh);
}

void parseArgs(int argc, char** argv) {
    if (argc < 4) {
        std::cerr << "Invalid useage, exit" << std::endl;
        exit(-1);
    }

    if (argv[1]) {
        hostFile = argv[1];
    } else {
        std::cerr << "No hostfile, exit" << std::endl;
        exit(-1);
    }

    if (argv[2]) {
        filename = argv[2];
    } else {
        std::cerr << "No filename, exit" << std::endl;
        exit(-1);
    }

    if(argv[3]) {
        mode = atoi(argv[3]);
    } else {
         std::cerr << "Failed to specify mode, exit" << std::endl;
        exit(-1);
    }

    if(argv[4]) {
        numDuplications = atoi(argv[4]);

        if(numDuplications <= 0) numDuplications = 1;

    } else {
        numDuplications = 1;
    }
}

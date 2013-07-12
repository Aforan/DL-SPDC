DL-SPDC
=======

A Scalable Parallel Data-intensive Computation library for the task scheduling of HPC MPI applications

Initialization sequences:
	MD:	SPDC_Init()
		SPDC_Init_MD_Server()
		SPDC_Build_Initial_Jobs()
		SPDC_Build_Locality_Map()
		SPDC_Distribute_Locality_Map()
		SPDC_Receive_Locality_Map()
		SPDC_Receive_Slave_Checkins()
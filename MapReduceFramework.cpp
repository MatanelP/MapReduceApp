//
// Created by Matanel on 27/04/2022.
//
# include "MapReduceFramework.h"


void emit2 (K2* key, V2* value, void* context){

}
void emit3 (K3* key, V3* value, void* context){

}

JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel){
  // tranforms inputVector to K2 and V2 vector for emit2
  // creating threads

  // creating JobHandler

}

void waitForJob(JobHandle job){

}
void getJobState(JobHandle job, JobState* state){
  // filling the state given according tto the jobHandler given
}
void closeJobHandle(JobHandle job){
  // finishing job
}

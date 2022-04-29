//
// Created by Matanel on 27/04/2022.
//
#include <cstdio>
#include <cstdlib>
#include <atomic>
#include <algorithm>
#include <iostream>
# include "MapReduceFramework.h"
#include "Barrier/Barrier.h"

struct ThreadContext {
  int threadID;
  stage_t stage;
  Barrier *barrier;
  const MapReduceClient *client;
  const InputVec *inputVec;
  OutputVec *outputVec;
  int totalThreadsCount;
  IntermediateVec **intermediateVectors;
  std::atomic<int> *mappingCounter;

};

struct Job {

};

void emit2 (K2 *key, V2 *value, void *context)
{
  ThreadContext *tc = (ThreadContext *) context;
  tc->intermediateVectors[tc->threadID]->push_back ({key, value});
}
void emit3 (K3 *key, V3 *value, void *context)
{

}

/**
 * function to be called upon by each thread, starting its map-reduce
 * algorithm part:
 * map -> sort -> |barrier| -> shuffle (for tid 0) -> reduce
 */
void *threadMapReduce (void *arg)
{
  ThreadContext *tc = (ThreadContext *) arg;
  //  mapping:
  tc->stage = MAP_STAGE;
  int index = (*(tc->mappingCounter))++;
  while (index < tc->inputVec->size ())
    {
      InputPair inputPair = tc->inputVec->at (index);
      tc->client->map (inputPair.first, inputPair.second, tc);
      index = (*(tc->mappingCounter))++;
    }
  // sorting:
  IntermediateVec *interVec = tc->intermediateVectors[tc->threadID];
  std::sort (interVec->begin (), interVec->end (),
             [] (IntermediatePair a, IntermediatePair b)
             {
               return *b.first < *a.first;
             });

  tc->barrier->barrier ();

  return 0;
}

JobHandle startMapReduceJob (const MapReduceClient &client,
                             const InputVec &inputVec, OutputVec &outputVec,
                             int multiThreadLevel)
{
  pthread_t threads[multiThreadLevel];
  ThreadContext contexts[multiThreadLevel];
  auto *barrier = new Barrier (multiThreadLevel);
  auto **intermediateVectors = new IntermediateVec *[multiThreadLevel];
  std::atomic<int> mappingCounter (0);


//    creating new intermediate vectors
  for (int i = 0; i < multiThreadLevel; ++i)
    {
      intermediateVectors[i] = new IntermediateVec ();
    }

//    init all contexts
  for (int i = 0; i < multiThreadLevel; ++i)
    {
      ThreadContext &context = contexts[i];
      context.threadID = i;
      context.stage = UNDEFINED_STAGE;
      context.barrier = barrier;
      context.client = &client;
      context.inputVec = &inputVec;
      context.outputVec = &outputVec;
      context.totalThreadsCount = multiThreadLevel;
      context.intermediateVectors = intermediateVectors;
      context.mappingCounter = &mappingCounter;
    }

  for (int i = 0; i < multiThreadLevel; ++i)
    {
      if (pthread_create (threads + i, NULL, threadMapReduce, contexts + i)
          != 0)
        {
//        todo - print error
          exit (EXIT_FAILURE);
        }
    }

  // creating JobHandler
  return (JobHandle) new Job;

}

void waitForJob (JobHandle job)
{

}
void getJobState (JobHandle job, JobState *state)
{
  // filling the state given according tto the jobHandler given
}
void closeJobHandle (JobHandle job)
{
  // finishing job
}

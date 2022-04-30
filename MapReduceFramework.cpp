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
  std::atomic<long> *counter;
  int *numOfIntermediatePairs;
  std::vector<IntermediateVec *> *shuffledVectors;

};

struct Job {

    ThreadContext* contexts_;
    int numOfThreads;
    int inputSize;

};

//// setting the first 2 bits of the atomic counter according to the stage given
//void setStageBitWise(std::atomic<long> *counter, stage_t stage){
//  switch (stage)
//    {
//      case UNDEFINED_STAGE:
//        counter->store (counter->load (),std::memory_order_relaxed);
//        break;
//      case MAP_STAGE:
//        break;
//      case SHUFFLE_STAGE:
//        break;
//      case REDUCE_STAGE:
//        break;
//    }
//}

void emit2 (K2 *key, V2 *value, void *context)
{
  ThreadContext *tc = (ThreadContext *) context;
  IntermediatePair intermediatePair = {key, value};
  tc->intermediateVectors[tc->threadID]->push_back (intermediatePair);
}
void emit3 (K3 *key, V3 *value, void *context)
{

}

void mapPhase (ThreadContext *tc)
{// setStageBitWise (tc->counter, MAP_STAGE);
  tc->stage = MAP_STAGE;
  long index = (*(tc->counter)).fetch_add (1);
  while (index < tc->inputVec->size ())
    {
      InputPair inputPair = tc->inputVec->at (index);
      tc->client->map (inputPair.first, inputPair.second, tc);
      index = (*(tc->counter)).fetch_add (1);
    }
}
void sortPhase (const ThreadContext *tc)
{
  IntermediateVec *interVec = tc->intermediateVectors[tc->threadID];
  *(tc->numOfIntermediatePairs) += (int) interVec->size ();
  std::sort (interVec->begin (), interVec->end (),
             [] (IntermediatePair a, IntermediatePair b)
             {
               return *b.first < *a.first;
             });
}
void shufflePhase (ThreadContext *tc)
{
  tc->stage = SHUFFLE_STAGE;
  if (tc->threadID == 0)
    {
      int sortedPairs = 0;
      for (int i = 0; i < tc->totalThreadsCount; i++)
        {
          IntermediateVec *currentVec = tc->intermediateVectors[i];
          while (!currentVec->empty ())
            {
              IntermediatePair pair = currentVec->back ();
              K2 *key = pair.first;
              auto *vecForKey = new IntermediateVec ();
              for (int j = 0; j < tc->totalThreadsCount; j++)
                {
                  IntermediateVec *interVec = tc->intermediateVectors[j];
                  if (interVec->empty ()) continue;
                  K2 *keyToAdd = interVec->at (interVec->size () - 1).first;
                  while (!interVec->empty () &&
                         !(*keyToAdd < *key || *key < *keyToAdd))
                    {
                      vecForKey->push_back (interVec->back ());
                      interVec->pop_back ();
                      sortedPairs++;
                      if (interVec->empty ()) break;
                      keyToAdd = interVec->at (interVec->size () - 1).first;
                    }
                }
              tc->shuffledVectors->push_back (vecForKey);
            }
        }
    }
}
/**
 * function to be called upon by each thread, starting its map-reduce
 * algorithm part:
 * map -> sort -> |barrier| -> shuffle (for tid 0) -> reduce
 */
void *threadMapReduce (void *arg)
{
  ThreadContext *tc = (ThreadContext *) arg;
  // mapping:
  mapPhase (tc);

  // sorting:
  sortPhase (tc);

  // barrier:
  tc->barrier->barrier ();

  // shuffle:
  shufflePhase (tc);

  // barrier (we can also use semaphore to make sure
  //          all threads are waiting for thread 0 to finish shuffling):
  tc->barrier->barrier ();

  // reduce:

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
  std::atomic<long> counter (0);
  int *numOfIntermediatePairs = new int (0);
  auto *shuffledVectors = new std::vector<IntermediateVec *> ();



  // creating new intermediate vectors
  for (int i = 0; i < multiThreadLevel; ++i)
    {
      intermediateVectors[i] = new IntermediateVec ();
    }

  // init all contexts
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
      context.counter = &counter;
      context.numOfIntermediatePairs = numOfIntermediatePairs;
      context.shuffledVectors = shuffledVectors;
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

  return (JobHandle) new Job{contexts,multiThreadLevel,(int) inputVec.size()};

}

void waitForJob (JobHandle job)
{

}
void getJobState (JobHandle job, JobState *state)
{
    Job* curr_job = (Job*) job;
    if (curr_job->contexts_[0].outputVec-> empty()){
        if (curr_job->contexts_[0].shuffledVectors->empty()){
            if (*(curr_job->contexts_[0].numOfIntermediatePairs) != 0){
                // in map phase, need to calculate percentage completion
                state->stage = MAP_STAGE;
                state->percentage = (float) curr_job->contexts_[0].inputVec->size() / (float) curr_job->inputSize;
            }
            else {
                // haven't started map phase yet
                state->stage = UNDEFINED_STAGE;
                state->percentage = 0;
            }
        }
        else{
            // in shuffle phase, need to calculate percentage of shuffle completion
            state->stage = SHUFFLE_STAGE;
            state->percentage = (float) curr_job->contexts_->shuffledVectors->size() / (float) curr_job->numOfThreads;

        }
    }
    else{
        // in reduce phase, need to calculate percentage of output vector completion
        state->stage = REDUCE_STAGE;
        state->percentage = (float) curr_job->contexts_[0].outputVec->size() /(float) curr_job->inputSize ;
    }



}
void closeJobHandle (JobHandle job)
{
  // finishing job
}

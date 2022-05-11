//
// Created by Matanel & Nadav on 27/04/2022.
//
#include <cstdio>
#include <cstdlib>
#include <atomic>
#include <algorithm>
#include <iostream>
#include "MapReduceFramework.h"
#include "Barrier.h"


#define JOIN_ERROR "Error while using pthread_join"
#define PTHREAD_CREATE_ERROR "Error while creating thread"
#define MUTEX_INIT_ERROR "Error while creating mutex"
#define LOCK_MUTEX_ERROR "Mutex lock error"
#define UNLOCK_MUTEX_ERROR "Mutex unlock error"

struct ThreadContext {
    int threadID;
    Barrier *barrier;
    const MapReduceClient *client;
    const InputVec *inputVec;
    OutputVec *outputVec;
    int totalThreadsCount;
    IntermediateVec **intermediateVectors;
    std::atomic<long> *counter;
    stage_t *curr_stage;
    int *numOfIntermediatePairs;
    int *numOfShuffledVecotrs;
    std::vector<IntermediateVec *> *shuffledVectors;
    pthread_mutex_t *mutex;

    pthread_t **threads;
};

struct Job {

    ThreadContext **contexts_;
    pthread_t **threads_;
    int numOfThreads;
    int inputSize;
    bool threadsFinishedFlag;
};

void lock_mutex (pthread_mutex_t* mutex){
    if (pthread_mutex_lock(mutex) != 0){
        std::cerr << LOCK_MUTEX_ERROR << std::endl;
        exit(EXIT_FAILURE);
    }
}

void unlock_mutex (pthread_mutex_t* mutex){
    if (pthread_mutex_unlock(mutex) != 0){
        std::cerr << UNLOCK_MUTEX_ERROR << std::endl;
        exit(EXIT_FAILURE);
    }
}

void emit2 (K2 *key, V2 *value, void *context)
{
    auto *tc = (ThreadContext *) context;
    IntermediatePair intermediatePair = {key, value};
    tc->intermediateVectors[tc->threadID]->push_back (intermediatePair);
}
void emit3 (K3 *key, V3 *value, void *context)
{
    auto *tc = (ThreadContext *) context;
    lock_mutex(tc->mutex);
    OutputPair outputPair = {key, value};
    tc->outputVec->push_back (outputPair);
    unlock_mutex(tc->mutex);
}

/**
 * Map Phase
 * @param tc Thread Context
 */
void mapPhase (ThreadContext *tc)
{
    lock_mutex(tc->mutex);
    if (*tc->curr_stage == UNDEFINED_STAGE){
        // first thread to reach mapPhase
        *tc->curr_stage = MAP_STAGE;
    }
    unlock_mutex(tc->mutex);
    long index = (*(tc->counter)).fetch_add (1);
    while (index < tc->inputVec->size ())
    {
        lock_mutex(tc->mutex);
        InputPair inputPair = tc->inputVec->at (index);
        tc->client->map (inputPair.first, inputPair.second, tc);
        index = (*tc->counter)++;
        unlock_mutex(tc->mutex);
    }
}

/**
 * Sort Phase
 * @param tc Thread Context
 */
void sortPhase (const ThreadContext *tc)
{
    lock_mutex(tc->mutex);
    IntermediateVec *interVec = tc->intermediateVectors[tc->threadID];
    *(tc->numOfIntermediatePairs) += (int) interVec->size ();
    unlock_mutex(tc->mutex);
    std::sort (interVec->begin (), interVec->end (),
               [] (IntermediatePair a, IntermediatePair b)
               {
                   return *b.first < *a.first;
               });
}


/**
 * Shuffle Phase
 * @param tc Thread context
 */

void shufflePhase (ThreadContext *tc)
{
    if (tc->threadID != 0){
        // ensuring only thread 0 runs shuffle
        return;
    }
    int sortedPairs = 0;
    lock_mutex(tc->mutex);
    *tc->curr_stage = SHUFFLE_STAGE;
    tc->counter->operator= (0);
    unlock_mutex(tc->mutex);

    for (int i = 0; i < tc->totalThreadsCount; i++)
    {
        IntermediateVec *currentVec = tc->intermediateVectors[i];
        while (!currentVec->empty ())
        {
            IntermediatePair pair = currentVec->back ();
            K2 *key = pair.first;
            auto vecForKey = new IntermediateVec;
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
                    lock_mutex(tc->mutex);
                    tc->counter->fetch_add (1);
                    unlock_mutex(tc->mutex);
                    if (interVec->empty ()) break;
                    keyToAdd = interVec->at (interVec->size () - 1).first;
                }
            }
            tc->shuffledVectors->push_back (vecForKey);
            (*tc->numOfShuffledVecotrs) += 1;
        }
    }
}


/**
 * Reduce Phase
 * @param tc Thread Context
 */
void reducePhase (ThreadContext *tc)
{
    lock_mutex(tc->mutex);
    if (*tc->curr_stage == SHUFFLE_STAGE){
        // first thread to hit reduce phase
        *tc->curr_stage = REDUCE_STAGE;
        tc->counter->operator=(0);
    }
    unlock_mutex(tc->mutex);

    while (true)
    {
        lock_mutex(tc->mutex);
        if (tc->shuffledVectors->empty()){
            unlock_mutex(tc->mutex);
            break;
        }
        IntermediateVec *vecForKey = tc->shuffledVectors->back ();
        tc->shuffledVectors->pop_back ();
        (*tc->numOfShuffledVecotrs) -= 1;
        unlock_mutex(tc->mutex);
        tc->client->reduce (vecForKey, tc);
        // reduction finished, adding number of pairs to counter
        tc->counter->fetch_add((int) vecForKey->size());
        delete vecForKey;
    }
}
/**
 * function to be called upon by each thread, starting its map-reduce
 * algorithm part:
 * map -> sort -> |barrier| -> shuffle (for tid 0) -> reduce
 */
void *threadMapReduce (void *arg)
{
    auto tc = (ThreadContext *) arg;
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
    reducePhase (tc);

    return 0;
}


JobHandle startMapReduceJob (const MapReduceClient &client,
                             const InputVec &inputVec, OutputVec &outputVec,
                             int multiThreadLevel)
{
    auto ** threads = new pthread_t* [multiThreadLevel];
    auto** contexts = new ThreadContext*[multiThreadLevel];
    auto *barrier = new Barrier (multiThreadLevel);
    auto **intermediateVectors = new IntermediateVec *[multiThreadLevel];
    auto* counter  = new std::atomic<long>(0);
    auto* curr_stage = new stage_t(UNDEFINED_STAGE);
    int *numOfIntermediatePairs = new int (0);
    int *numOfShuffledVectors = new int(0);
    auto shuffledVectors = new std::vector<IntermediateVec *>;
    auto *mutex = new pthread_mutex_t;

    if (pthread_mutex_init (mutex, nullptr) != 0)
    {
        std::cerr << MUTEX_INIT_ERROR << std::endl;
        exit (EXIT_FAILURE);
    }



    // creating new intermediate vectors
    for (int i = 0; i < multiThreadLevel; ++i)
    {
        contexts[i] = new ThreadContext;
        intermediateVectors[i] = new IntermediateVec;
        threads[i] = new pthread_t;
    }

    // init all contexts
    for (int i = 0; i < multiThreadLevel; ++i)
    {
        ThreadContext* context = contexts[i];
        context->threadID = i;
        context->barrier = barrier;
        context->client = &client;
        context->inputVec = &inputVec;
        context->outputVec = &outputVec;
        context->totalThreadsCount = multiThreadLevel;
        context->intermediateVectors = intermediateVectors;
        context->counter = counter;
        context->curr_stage = curr_stage;
        context->numOfIntermediatePairs = numOfIntermediatePairs;
        context->numOfShuffledVecotrs = numOfShuffledVectors;
        context->shuffledVectors = shuffledVectors;
        context->mutex = mutex;

        context->threads = threads;

    }

    for (int i = 0; i < multiThreadLevel; ++i)
    {
        if (pthread_create (*(threads + i), nullptr, threadMapReduce, *(contexts + i))
            != 0)
        {
            std::cerr << PTHREAD_CREATE_ERROR << std::endl;
            exit (EXIT_FAILURE);
        }
    }


    // creating JobHandler

    return (JobHandle) new Job{contexts, threads,multiThreadLevel,
                               (int) inputVec.size (), false};

}

void waitForJob (JobHandle job)
{
    Job* curr_jub = (Job*) job;
    if (!curr_jub->threadsFinishedFlag){
        for (int i = 0; i < curr_jub->numOfThreads; ++i){
            if ((pthread_join(*curr_jub->contexts_[i]->threads[i], nullptr)) <0){
                std::cerr << JOIN_ERROR << std::endl;
                exit(EXIT_FAILURE);
            }
        }
        curr_jub->threadsFinishedFlag = true;
    }




}

void getJobState (JobHandle job, JobState *state)
{


    Job *curr_job = (Job *) job;
    lock_mutex(curr_job->contexts_[0]->mutex);

    stage_t curr_stage = *curr_job->contexts_[0]->curr_stage;
    auto numerator = (float) *curr_job->contexts_[0]->counter;
    float denominator = 0.0f;
    state->stage = curr_stage;
    switch(curr_stage){
        case UNDEFINED_STAGE:
            break;
        case MAP_STAGE:
            denominator = (float) curr_job->inputSize;
            if (numerator > denominator){
                numerator = denominator;
            }
            break;
        case SHUFFLE_STAGE: case REDUCE_STAGE:
            denominator = (float) *curr_job->contexts_[0]->numOfIntermediatePairs;
            break;

    }

    if (denominator == 0){
        state->percentage = 0;
    }
    else{
        state->percentage = (numerator / denominator) * 100;
    }

    unlock_mutex(curr_job->contexts_[0]->mutex);

}


void closeJobHandle (JobHandle job)
{
    // finishing job
    waitForJob(job);
    Job* curr_job = (Job*) job;


    delete curr_job->contexts_[0]->counter;
    delete curr_job->contexts_[0]->curr_stage;
    delete curr_job->contexts_[0]->shuffledVectors;
    delete curr_job->contexts_[0]->numOfIntermediatePairs;
    delete curr_job->contexts_[0]->barrier;

    pthread_mutex_destroy(curr_job->contexts_[0]->mutex);
    delete curr_job->contexts_[0]->mutex;

    for (int i = 0; i < curr_job->numOfThreads; ++i){
        delete curr_job->contexts_[i]->intermediateVectors[i];
    }
    delete[] curr_job->contexts_[0]->intermediateVectors;

    for (int i = 0; i < curr_job->numOfThreads; ++i){
        delete curr_job->contexts_[i]->threads[i];
        delete curr_job->contexts_[i];
    }
    delete[] curr_job->contexts_;
    delete[] curr_job->threads_;
    delete curr_job;

}
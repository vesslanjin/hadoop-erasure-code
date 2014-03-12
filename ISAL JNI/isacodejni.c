/**********************************************************************
INTEL CONFIDENTIAL
Copyright 2012 Intel Corporation All Rights Reserved.

The source code contained or described herein and all documents
related to the source code ("Material") are owned by Intel Corporation
or its suppliers or licensors. Title to the Material remains with
Intel Corporation or its suppliers and licensors. The Material may
contain trade secrets and proprietary and confidential information of
Intel Corporation and its suppliers and licensors, and is protected by
worldwide copyright and trade secret laws and treaty provisions. No
part of the Material may be used, copied, reproduced, modified,
published, uploaded, posted, transmitted, distributed, or disclosed in
any way without Intel's prior express written permission.

No license under any patent, copyright, trade secret or other
intellectual property right is granted to or conferred upon you by
disclosure or delivery of the Materials, either expressly, by
implication, inducement, estoppel or otherwise. Any license under such
intellectual property rights must be express and approved by Intel in
writing.

Unless otherwise agreed by Intel in writing, you may not remove or
alter this notice or any other notice embedded in Materials by Intel
or Intel's suppliers or licensors in any way.
**********************************************************************/

#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>  // for memset, memcmp

#include "include/erasure_code.h"
#include "include/types.h"
#include "include/gf_vect_mul.h"
#include "org_apache_hadoop_raid_ReedSolomonCode.h"
#include <jni.h>
#include <pthread.h>
#include <signal.h>

#define MMAX 30
#define KMAX 20

typedef unsigned char u8;

static pthread_key_t keyCEn;
static pthread_key_t keyCDe;
static pthread_once_t key_once = PTHREAD_ONCE_INIT;

typedef struct _codec_parameter{
    int paritySize;
    int stripeSize;
    u8 a[MMAX*KMAX];
    u8 b[MMAX*KMAX];
    u8 d[MMAX*KMAX];
    u8 g_tbls[MMAX*KMAX*32];
    u8 ** data;
    u8 ** code;
    jobject * codebuf;
}Codec_Parameter;

static void make_key(){
    (void) pthread_key_create(&keyCEn, NULL);
    (void) pthread_key_create(&keyCDe, NULL);
}

JNIEXPORT jint JNICALL Java_org_apache_hadoop_raid_ReedSolomonCode_isaEnInit
  (JNIEnv *env, jclass myclass, jint stripeSize, jint paritySize, jintArray matrix){
        Codec_Parameter * pCodecParameter = NULL;
        jint * jmatrix = NULL;
        pthread_once(&key_once, make_key);

        if(NULL == (pCodecParameter = (Codec_Parameter *)pthread_getspecific(keyCEn))){
            pCodecParameter = (Codec_Parameter *)malloc(sizeof(Codec_Parameter));
            if(!pCodecParameter){
                printf("Out of memory in ISA encoder init\n");
                return -1;
            }

            if (stripeSize > KMAX || paritySize > (MMAX - KMAX)){
                printf("max stripe size is %d and max parity size is %d\n", KMAX, MMAX - KMAX);
                return -2;
            }

            int totalSize = paritySize + stripeSize;
            pCodecParameter->paritySize = paritySize;
            pCodecParameter->stripeSize = stripeSize;
            pCodecParameter->data = (u8 **)malloc(sizeof(u8 *) * stripeSize);
            pCodecParameter->code = (u8 **)malloc(sizeof(u8*) * paritySize);
            pCodecParameter->codebuf = (jobject *)malloc(sizeof(jobject) * paritySize);

            int i, j;
            jmatrix = env->GetIntArrayElements(matrix, false);
            memset(pCodecParameter->a, 0, stripeSize*totalSize);
            for(i=0; i<stripeSize; i++){
               pCodecParameter->a[stripeSize*i + i] = 1;
            }
            for(i=stripeSize; i<totalSize; i++){
               for(j=0; j<stripeSize; j++){
                    pCodecParameter->a[stripeSize*i+j] = jmatrix[stripeSize*(i-stripeSize)+j];
               }
            }

            ec_init_tables(stripeSize, paritySize, &(pCodecParameter->a)[stripeSize * stripeSize], pCodecParameter->g_tbls);
            
            (void) pthread_setspecific(keyCEn, pCodecParameter);
        }
        return 0;
  }

JNIEXPORT jint JNICALL Java_org_apache_hadoop_raid_ReedSolomonCode_isaEncode
  (JNIEnv * env, jclass myclass, jobjectArray data, jobjectArray code, jint blockSize){

        int dataLen, codeLen;
        Codec_Parameter * pCodecParameter = NULL;
        pthread_once(&key_once, make_key);

        if(NULL == (pCodecParameter = (Codec_Parameter *)pthread_getspecific(keyCEn))){
            printf("ISA encoder not initilized!\n");
            return -3;
        }
        dataLen = env->GetArrayLength(data);
        codeLen = env->GetArrayLength(code);

        if(dataLen != pCodecParameter->stripeSize){
            printf("wrong stripe size, expect %d but got %d\n", pCodecParameter->stripeSize, dataLen);
            return -4;
        }
        if(codeLen != pCodecParameter->paritySize){
            printf("wrong paritySize, expect %d but got %d\n", pCodecParameter->paritySize, codeLen);
            return -5;
        }
        int stripeSize = pCodecParameter->stripeSize;
        int paritySize = pCodecParameter->paritySize;

        for(int k = 0; k < dataLen; k++){
            pCodecParameter->data[k] = (u8 *)env->GetDirectBufferAddress(env->GetObjectArrayElement(data, k));
        }
        for(int m = 0; m < codeLen; m++){
            pCodecParameter->codebuf[m] = env->GetObjectArrayElement(code, m);
            pCodecParameter->code[m] = (u8 *)env->GetDirectBufferAddress(pCodecParameter->codebuf[m]);
        }
        
        ec_encode_data(blockSize, stripeSize, paritySize, pCodecParameter->g_tbls, pCodecParameter->data, pCodecParameter->code);
        for(int m = 0; m < codeLen; m++){
            env->SetObjectArrayElement(code, m, pCodecParameter->codebuf[m]);
        }
        return 0;
  }

JNIEXPORT jint JNICALL Java_org_apache_hadoop_raid_ReedSolomonCode_isaEnEnd
  (JNIEnv * env, jclass myclass){
        Codec_Parameter * pCodecParameter = NULL;
        pthread_once(&key_once, make_key);

        if(NULL == (pCodecParameter = (Codec_Parameter *)pthread_getspecific(keyCEn))){
            printf("ISA encoder not initilized!\n");
            return -6;
        }
        
        free(pCodecParameter->data);
        free(pCodecParameter->code);
        free(pCodecParameter->codebuf);
        free(pCodecParameter);
        (void)pthread_setspecific(keyCEn, NULL);
        return 0;
  }


typedef struct _codec_parameter_de{
    int paritySize;
    int stripeSize;
    u8 a[MMAX*KMAX];
    u8 b[MMAX*KMAX];
    u8 c[MMAX*KMAX];
    u8 d[MMAX*KMAX];
    u8 e[MMAX*KMAX];
    u8 g_tbls[MMAX*KMAX*32];
    u8 * recov[MMAX];
    u8 *temp_buffs[MMAX];
    u8 ** data;
    u8 ** code;
    jobject * datajbuf;
    int * erasured;
}Codec_Parameter_de;

JNIEXPORT jint JNICALL Java_org_apache_hadoop_raid_ReedSolomonCode_isaDeInit
  (JNIEnv *env, jclass myclass, jint stripeSize, jint paritySize, jintArray matrix){

        Codec_Parameter_de * pCodecParameter = NULL;
        jint * jmatrix = NULL;

        pthread_once(&key_once, make_key);

        if(NULL == (pCodecParameter = (Codec_Parameter_de *)pthread_getspecific(keyCDe))){
            pCodecParameter = (Codec_Parameter_de *)malloc(sizeof(Codec_Parameter_de));
            if(!pCodecParameter){
                printf("Out of memory in ISA decoder init\n");
                return -1;
            }

            if (stripeSize > KMAX || paritySize > (MMAX - KMAX)){
                printf("max stripe size is %d and max parity size is %d\n", KMAX, MMAX - KMAX);
                return -2;
            }

            int totalSize = paritySize + stripeSize;
            pCodecParameter->paritySize = paritySize;
            pCodecParameter->stripeSize = stripeSize;
            pCodecParameter->data = (u8 **)malloc(sizeof(u8 *) * (stripeSize + paritySize));
            pCodecParameter->code = (u8 **)malloc(sizeof(u8 *) * (paritySize));
            pCodecParameter->datajbuf = (jobject *)malloc(sizeof(jobject) * (stripeSize + paritySize));
            pCodecParameter->erasured = (int *)malloc(sizeof(int) * (stripeSize + paritySize));
            pCodecParameter->erasured[0] = 0; 

            int i, j;
            jmatrix = env->GetIntArrayElements(matrix, false);
            memset(pCodecParameter->a, 0, stripeSize*totalSize);
            for(i=0; i<stripeSize; i++){
                 pCodecParameter->a[stripeSize*i + i] = 1;
            }
            for(i=stripeSize; i<totalSize; i++){
                for(j=0; j<stripeSize; j++){
                   pCodecParameter->a[stripeSize*i+j] = jmatrix[stripeSize*(i-stripeSize)+j];
                }
            }
            
            ec_init_tables(stripeSize, paritySize, &(pCodecParameter->a)[stripeSize * stripeSize], pCodecParameter->g_tbls);
            (void) pthread_setspecific(keyCDe, pCodecParameter);
        }
        return 0;
 }

JNIEXPORT jint JNICALL Java_org_apache_hadoop_raid_ReedSolomonCode_isaDecode
  (JNIEnv *env, jclass myclass, jobjectArray alldata, jintArray erasures, jint blocksize){

      Codec_Parameter_de * pCodecParameter = NULL;
      pthread_once(&key_once, make_key);
      jboolean isCopy;

      int * erasured = NULL;
      int i, j, p, r, k, errorlocation;
      int alldataLen = env->GetArrayLength(alldata);
      int erasureLen = env->GetArrayLength(erasures);
      int src_error_list[erasureLen];
      u8 s;

// Check all the parameters.

      if(NULL == (pCodecParameter = (Codec_Parameter_de *)pthread_getspecific(keyCDe))){
          printf("ReedSolomonDecoder DE not initilized!\n");
          return -3;
      }

      if(erasureLen > pCodecParameter->paritySize){
          printf("Too many erasured data!\n");
          return -4;
      }

      if(alldataLen != pCodecParameter->stripeSize + pCodecParameter->paritySize){
          printf("Wrong data and parity data size.\n");
          return -5;
      }

      for(j = 0; j < pCodecParameter->stripeSize + pCodecParameter->paritySize; j++){
          pCodecParameter->erasured[j] = -1;
      }

      int * tmp = (int *)env->GetIntArrayElements(erasures, &isCopy);

      int parityErrors = 0; 


      for(j = 0; j < erasureLen; j++){
          if (tmp[j] >= pCodecParameter->paritySize) {  // errors in parity will not be fixed
              errorlocation = tmp[j] - pCodecParameter->paritySize;
              pCodecParameter->erasured[errorlocation] = 1;
          }
          else if (tmp[j] >= 0){ // put error parity postion
              pCodecParameter->erasured[tmp[j] + pCodecParameter->stripeSize] = 1;
              parityErrors++;
          }

      }
      
      // make the src_error_list in the right order
      for(j = 0, r = 0; j < pCodecParameter->paritySize + pCodecParameter->stripeSize; j++ ) {
          if(pCodecParameter->erasured[j] == 1)    src_error_list[r++] = j ;
      }

      for(j = pCodecParameter->paritySize, i = 0, r = 0; j < pCodecParameter->paritySize + pCodecParameter->stripeSize; j++){
          pCodecParameter->datajbuf[j] = env->GetObjectArrayElement(alldata, j);
          pCodecParameter->data[j] = (u8 *)env->GetDirectBufferAddress(pCodecParameter->datajbuf[j]);
          if(pCodecParameter->erasured[j - pCodecParameter->paritySize] == -1){
               pCodecParameter->recov[r++] = pCodecParameter->data[j];
          }
          else{
               pCodecParameter->code[i++] = pCodecParameter->data[j];
          }
      }
//first parity length elements in alldata are saving parity data 
      for (j = 0; j < pCodecParameter->paritySize ; j++){
          pCodecParameter->datajbuf[j] = env->GetObjectArrayElement(alldata, j);
          pCodecParameter->data[j] = (u8 *)env->GetDirectBufferAddress(pCodecParameter->datajbuf[j]);
          if(pCodecParameter->erasured[j + pCodecParameter->stripeSize] == -1) {
              pCodecParameter->recov[r++] = pCodecParameter->data[j];
          } else {
              pCodecParameter->code[i++] = pCodecParameter->data[j];
          }
      }

      for(i = 0, r = 0; i < pCodecParameter->stripeSize; i++, r++){
          while(pCodecParameter->erasured[r] == 1) r++;
            for(j = 0; j < pCodecParameter->stripeSize; j++){
                 pCodecParameter->b[pCodecParameter->stripeSize * i + j] = 
                            pCodecParameter->a[pCodecParameter->stripeSize * r + j];
            }
      }

      //Construct d, the inverted matrix.

      if(gf_invert_matrix(pCodecParameter->b, pCodecParameter->d, pCodecParameter->stripeSize) < 0){
          printf("BAD MATRIX!\n");
          return -6;
      }
      int srcErrors = erasureLen - parityErrors;

      for(i = 0; i < srcErrors; i++){
          for(j = 0; j < pCodecParameter->stripeSize; j++){
              //store all the erasured line numbers's to the c. 
              pCodecParameter->c[pCodecParameter->stripeSize * i + j] = 
                    pCodecParameter->d[pCodecParameter->stripeSize * src_error_list[i] + j];
          }
      }

      // recover data
      for(i = srcErrors, p = 0; i < erasureLen; i++, p++) {
         for(j = 0; j < pCodecParameter->stripeSize; j++)  {
            pCodecParameter->e[pCodecParameter->stripeSize * p + j] = pCodecParameter->a[pCodecParameter->stripeSize * src_error_list[i] + j];
         }
      }
      
      // e * invert - b
      for(p = 0; p < parityErrors; p++) {
         for(i = 0; i < pCodecParameter->stripeSize; i++) {
            s = 0; 
            for(j = 0; j < pCodecParameter->stripeSize; j++)
              s ^= gf_mul(pCodecParameter->d[j * pCodecParameter->stripeSize + i], pCodecParameter->e[pCodecParameter->stripeSize * p + j]);
            pCodecParameter->c[pCodecParameter->stripeSize * (srcErrors + p) + i] = s;
         }
      }
      ec_init_tables(pCodecParameter->stripeSize, erasureLen, pCodecParameter->c, pCodecParameter->g_tbls);

    // Get all the repaired data into pCodecParameter->data, in the first erasuredLen rows.
      ec_encode_data(blocksize, pCodecParameter->stripeSize, erasureLen, pCodecParameter->g_tbls, 
                        pCodecParameter->recov, pCodecParameter->code);

    // Set the repaired data to alldata. 
      for(j = 0; j < pCodecParameter->stripeSize + pCodecParameter->paritySize ; j++){
          if(pCodecParameter->erasured[j - pCodecParameter->paritySize] != -1){
              env->SetObjectArrayElement(alldata, j, pCodecParameter->datajbuf[j]);
          }
      }
      
      if(isCopy){
          env->ReleaseIntArrayElements(erasures, (jint *)tmp, JNI_ABORT);
      }
      return 0;
 }

JNIEXPORT jint JNICALL Java_org_apache_hadoop_raid_ReedSolomonCode_isaDeEnd
  (JNIEnv *env, jclass myclass){
      Codec_Parameter_de * pCodecParameter = NULL;
      if(NULL == (pCodecParameter = (Codec_Parameter_de *)pthread_getspecific(keyCDe))){
          printf("ISA decoder not initilized!\n");
          return -7;
      }

      free(pCodecParameter->data);
      free(pCodecParameter->datajbuf);
      free(pCodecParameter->code);
      free(pCodecParameter->erasured);
      free(pCodecParameter);
      (void)pthread_setspecific(keyCDe, NULL);
      return 0;

  }

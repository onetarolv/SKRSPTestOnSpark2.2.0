/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.util.random

import scala.collection.mutable.ArrayBuffer
import scala.math._
import scala.reflect.ClassTag
import scala.util.Random

private[spark] object SamplingUtils {

  /**
   * Reservoir sampling implementation that also returns the input size.
   *
   * @param input input size
   * @param k reservoir size
   * @param seed random seed
   * @return (samples, input size)
   */
  def reservoirSampleAndCount[T: ClassTag](
      input: Iterator[T],
      k: Int,
      seed: Long = Random.nextLong())
    : (Array[T], Long) = {
    val reservoir = new Array[T](k)
    // Put the first k elements in the reservoir.
    var i = 0
    while (i < k && input.hasNext) {
      val item = input.next()
      reservoir(i) = item
      i += 1
    }

    // If we have consumed all the elements, return them. Otherwise do the replacement.
    if (i < k) {
      // If input size < k, trim the array to return only an array of input size.
      val trimReservoir = new Array[T](i)
      System.arraycopy(reservoir, 0, trimReservoir, 0, i)
      (trimReservoir, i)
    } else {
      // If input size > k, continue the sampling process.
      var l = i.toLong
      val rand = new XORShiftRandom(seed)
      while (input.hasNext) {
        val item = input.next()
        l += 1
        // There are k elements in the reservoir, and the l-th element has been
        // consumed. It should be chosen with probability k/l. The expression
        // below is a random long chosen uniformly from [0,l)
        val replacementIndex = (rand.nextDouble() * l).toLong
        if (replacementIndex < k) {
          reservoir(replacementIndex.toInt) = item
        }
      }
      (reservoir, l)
    }
  }

  /**
   * Returns a sampling rate that guarantees a sample of size greater than or equal to
   * sampleSizeLowerBound 99.99% of the time.
   *
   * How the sampling rate is determined:
   *
   * Let p = num / total, where num is the sample size and total is the total number of
   * datapoints in the RDD. We're trying to compute q {@literal >} p such that
   *   - when sampling with replacement, we're drawing each datapoint with prob_i ~ Pois(q),
   *     where we want to guarantee
   *     Pr[s {@literal <} num] {@literal <} 0.0001 for s = sum(prob_i for i from 0 to total),
   *     i.e. the failure rate of not having a sufficiently large sample {@literal <} 0.0001.
   *     Setting q = p + 5 * sqrt(p/total) is sufficient to guarantee 0.9999 success rate for
   *     num {@literal >} 12, but we need a slightly larger q (9 empirically determined).
   *   - when sampling without replacement, we're drawing each datapoint with prob_i
   *     ~ Binomial(total, fraction) and our choice of q guarantees 1-delta, or 0.9999 success
   *     rate, where success rate is defined the same as in sampling with replacement.
   *
   * The smallest sampling rate supported is 1e-10 (in order to avoid running into the limit of the
   * RNG's resolution).
   *
   * @param sampleSizeLowerBound sample size
   * @param total size of RDD
   * @param withReplacement whether sampling with replacement
   * @return a sampling rate that guarantees sufficient sample size with 99.99% success rate
   */
  def computeFractionForSampleSize(sampleSizeLowerBound: Int, total: Long,
      withReplacement: Boolean): Double = {
    if (withReplacement) {
      PoissonBounds.getUpperBound(sampleSizeLowerBound) / total
    } else {
      val fraction = sampleSizeLowerBound.toDouble / total
      BinomialBounds.getUpperBound(1e-4, total, fraction)
    }
  }

  def v_prime(u: Double, n: Double): Double = {
    pow(u, 1.0/ n)
  }

  def rejectionSampleAndCount[T: ClassTag](
  input: Iterator[T],
  sampleSizeLowerBound: Int,
  sampleSizeUpperBound: Int,
  alpha: Double,
  len: Long,
  sampleRate: Double)
  : (Array[T], Double) = {
    val starttime = System.currentTimeMillis()
    val iter = input
    //val len = seq.length //numbers of intput datapoints
    var n: Double = {
      val size = (sampleRate * len).toInt.toDouble
      if(size < sampleSizeLowerBound){
        sampleSizeLowerBound
      }else if(size > sampleSizeUpperBound){
        sampleSizeUpperBound
      }else{
        size
      }
    }
    //println("n = " + n)

    var N = len.toDouble
    val fraction = n / len //SamplingUtils.computeFractionForSampleSize(sampleSizeLowerBound, len, false) //sampleSizeLowerBound.toDouble/len.toDouble
    //var n = (N * fraction).toInt.toDouble
    val threshold = (1/alpha) * n
    var i = 0
    val rng = new Random()
    var step = 0
    val sample = new ArrayBuffer[T]()
    var quan1 = N - n + 1
    var quan2 = quan1 / N
    var u = rng.nextDouble()
    var vprime = v_prime(u, n)

    while(n > 1 && iter.hasNext && (1/alpha) * n < N){

      var flag = false
      var r = N * (1.0 - vprime)
      var step = r.toInt
      val valnum = N - n - 1
      while(step >= valnum){
        u = rng.nextDouble()
        vprime = v_prime(u, n)
        r = N * (1.0 - v_prime(u, n))
        step = r.toInt
      }
      //make sure that step < N - n - 1
      val v = rng.nextDouble()
      //var y = rng.nextDouble()/ quan2
      val y = (v * N)/valnum
      val LHS = v_prime(y, n - 1)
      //val RHS = ((quan1 - step)/quan1) * (N / (N - x))
      val RHS = (1 - step / (n - 1)) * (N / (N - r))
      if(LHS <= RHS){
        flag = true
      }else{
        val (min, numerator ) = {
          if(n - 1 > step){
            ( N - step, n - 1)
          }else{
            ( valnum, step.toDouble) //denominator
          }
        }
        var denominator = N - 1
        var fs = 1/y
        while(denominator >= min){
          fs = fs * (1 - (numerator / denominator))
          denominator = denominator - 1
        }
        val LFS = v_prime(fs, n - 1)
        val RFS = 1 - r/N
        if(LFS <= RFS){
          flag = true
          vprime = v_prime(rng.nextDouble(), n - 1)
        }else{
          vprime = v_prime(rng.nextDouble(), n)
        }
        //
      }
      if(flag){
        addSampleItem(step, iter, sample)
        N = N - 1 - step
        n = n - 1
        i = i + step + 1
      }
    }
    if(n > 1){
      //call simpler step-based algorithm
      stepSimpleSmaple(N, n, i, iter, sample)
    }else{
      step = (N * vprime).toInt
      addSampleItem(step, iter, sample)
    }
    (sample.toArray, fraction)
  }
  def addSampleItem[T: ClassTag](step: Int, iter: Iterator[T], sample: ArrayBuffer[T]): Unit ={
    var stmp = step
    while(iter.hasNext && stmp > 0){
      stmp = stmp - 1
      iter.next()
    }
    sample.append(iter.next())
  }
  def stepSimpleSmaple[T: ClassTag](valN: Double, valn: Double, vali: Int, iter : Iterator[T], sample: ArrayBuffer[T]): Unit ={
    var N = valN
    var n = valn
    var i = vali
    val rng = new Random()
    var flag = true
    var step = 0
    while(n >= 2 && flag){
      val v = rng.nextDouble()
      step = 0
      var prob = 1 - n/N
      while(prob > v){
        step = step + 1
        N=N-1
        prob = prob * (1 - n/N)
      }
      var stmp = step
      while(iter.hasNext && stmp > 0){
        stmp = stmp - 1
        iter.next()
      }
      if(iter.hasNext){
        val item = iter.next()
        sample.append(item)
        N = N - 1 - step
        n = n - 1
      }else{
        flag = false
      }
      //println(step + ":"  + item)
    }
    if(n == 1){
      step = (N * rng.nextDouble()).toInt
      addSampleItem(step, iter, sample)

    }
  }
}

/**
 * Utility functions that help us determine bounds on adjusted sampling rate to guarantee exact
 * sample sizes with high confidence when sampling with replacement.
 */
private[spark] object PoissonBounds {

  /**
   * Returns a lambda such that Pr[X {@literal >} s] is very small, where X ~ Pois(lambda).
   */
  def getLowerBound(s: Double): Double = {
    math.max(s - numStd(s) * math.sqrt(s), 1e-15)
  }

  /**
   * Returns a lambda such that Pr[X {@literal <} s] is very small, where X ~ Pois(lambda).
   *
   * @param s sample size
   */
  def getUpperBound(s: Double): Double = {
    math.max(s + numStd(s) * math.sqrt(s), 1e-10)
  }

  private def numStd(s: Double): Double = {
    // TODO: Make it tighter.
    if (s < 6.0) {
      12.0
    } else if (s < 16.0) {
      9.0
    } else {
      6.0
    }
  }
}

/**
 * Utility functions that help us determine bounds on adjusted sampling rate to guarantee exact
 * sample size with high confidence when sampling without replacement.
 */
private[spark] object BinomialBounds {

  val minSamplingRate = 1e-10

  /**
   * Returns a threshold `p` such that if we conduct n Bernoulli trials with success rate = `p`,
   * it is very unlikely to have more than `fraction * n` successes.
   */
  def getLowerBound(delta: Double, n: Long, fraction: Double): Double = {
    val gamma = - math.log(delta) / n * (2.0 / 3.0)
    fraction + gamma - math.sqrt(gamma * gamma + 3 * gamma * fraction)
  }

  /**
   * Returns a threshold `p` such that if we conduct n Bernoulli trials with success rate = `p`,
   * it is very unlikely to have less than `fraction * n` successes.
   */
  def getUpperBound(delta: Double, n: Long, fraction: Double): Double = {
    val gamma = - math.log(delta) / n
    math.min(1,
      math.max(minSamplingRate, fraction + gamma + math.sqrt(gamma * gamma + 2 * gamma * fraction)))
  }
}

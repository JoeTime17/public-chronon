package ai.chronon.online.stats
import ai.chronon.observability.DriftMetric
import org.apache.datasketches.kll.KllFloatsSketch

import scala.math._

object DriftMetrics {
  case class LpDistances(linf: Double, l2: Double, l1: Double) {
    def asJavaMap: java.util.Map[String, java.lang.Double] = {
      val result = new java.util.LinkedHashMap[String, java.lang.Double]()
      result.put("linf-distance", linf)
      result.put("l2-distance", l2)
      result.put("l1-distance", l1)
      result
    }
  }

  /** Computes L-inf, L2, and L1 distances between two count histograms after normalizing each to probability mass.
    *
    * The histogram keys are treated as categorical buckets. Missing buckets are interpreted as zero probability.
    * This is used for low-cardinality feature histograms and boolean true/false/null distributions.
    */
  def histogramLpDistances(reference: java.util.Map[String, _ <: Number],
                           comparison: java.util.Map[String, _ <: Number]): LpDistances = {
    require(reference != null && comparison != null, "Both histograms are required")

    val referenceSum = sumHistogram(reference)
    val comparisonSum = sumHistogram(comparison)
    if (referenceSum <= 0.0 || comparisonSum <= 0.0) {
      throw new IllegalArgumentException(
        s"Histogram counts must be positive: referenceSum=$referenceSum, comparisonSum=$comparisonSum")
    }

    val keys = new java.util.HashSet[String]()
    keys.addAll(reference.keySet())
    keys.addAll(comparison.keySet())

    val it = keys.iterator()
    var linf = 0.0
    var l1 = 0.0
    var l2Squared = 0.0
    while (it.hasNext) {
      val key = it.next()
      val diff = math.abs(
        histogramProbability(reference, key, referenceSum) -
          histogramProbability(comparison, key, comparisonSum))
      linf = math.max(linf, diff)
      l1 += diff
      l2Squared += diff * diff
    }

    LpDistances(linf, math.sqrt(l2Squared), l1)
  }

  private def sumHistogram(histogram: java.util.Map[String, _ <: Number]): Double = {
    var result = 0.0
    val it = histogram.keySet().iterator()
    while (it.hasNext) {
      val key = it.next()
      val value = histogram.get(key)
      if (value != null) {
        result += validateHistogramBin(key, value)
      }
    }
    result
  }

  private def histogramProbability(histogram: java.util.Map[String, _ <: Number],
                                   key: String,
                                   total: Double): Double = {
    if (total <= 0.0 || total.isNaN || total.isInfinite) {
      throw new IllegalArgumentException(s"Histogram total must be positive and finite: total=$total")
    }
    val value = histogram.get(key)
    if (value == null) 0.0 else validateHistogramBin(key, value) / total
  }

  private def validateHistogramBin(key: String, value: Number): Double = {
    val doubleValue = value.doubleValue()
    if (doubleValue.isNaN || doubleValue.isInfinite || doubleValue < 0.0) {
      throw new IllegalArgumentException(s"Invalid histogram bin: key=$key, value=$doubleValue")
    }
    doubleValue
  }

  /** Computes distances between two KLL-backed distributions.
    *
    * Split points are chosen from both sketches' quantiles, so the comparison uses the detail retained in the
    * sketches without requiring raw samples. Low-mass PMF bins are treated as sketch noise and dropped before
    * renormalization; this keeps tiny KLL boundary artifacts from dominating the L1/L2 values.
    */
  def kllSketchDistances(reference: KllFloatsSketch,
                         comparison: KllFloatsSketch,
                         bins: Int = 50,
                         minSketchN: Long = 20L,
                         noiseFloor: Double = 1e-4): LpDistances = {
    require(reference != null && comparison != null, "Both sketches are required")
    require(bins >= 2, "bins must be at least 2")
    require(noiseFloor >= 0.0 && noiseFloor < 1.0, "noiseFloor must be in [0, 1)")

    if (reference.getN < minSketchN || comparison.getN < minSketchN) {
      throw new IllegalArgumentException(
        s"Not enough sketch data to compute robust drift: referenceN=${reference.getN}, comparisonN=${comparison.getN}, minSketchN=$minSketchN")
    }

    val fractions = (0 to bins).map(_.toDouble / bins).toArray
    val distinctQuantiles = (reference.getQuantiles(fractions) ++ comparison.getQuantiles(fractions))
      .map(_.toDouble)
      .filter(v => !v.isNaN && !v.isInfinite)
      .distinct
      .sorted
    val splitPoints = distinctQuantiles
      .zip(distinctQuantiles.drop(1))
      .map { case (left, right) => ((left + right) * 0.5).toFloat }

    val (referencePmf, comparisonPmf) =
      denoisePmfs(reference.getPMF(splitPoints), comparison.getPMF(splitPoints), noiseFloor)

    var i = 0
    var linfPmf = 0.0
    var l1 = 0.0
    var l2Squared = 0.0
    while (i < referencePmf.length) {
      val diff = math.abs(referencePmf(i) - comparisonPmf(i))
      linfPmf = math.max(linfPmf, diff)
      l1 += diff
      l2Squared += diff * diff
      i += 1
    }

    LpDistances(
      linf = math.max(linfPmf, lInfCdfDistance(reference, comparison, splitPoints)),
      l2 = math.sqrt(l2Squared),
      l1 = l1
    )
  }

  private def lInfCdfDistance(reference: KllFloatsSketch,
                              comparison: KllFloatsSketch,
                              splitPoints: Array[Float]): Double = {
    var linf = 0.0
    var i = 0
    while (i < splitPoints.length) {
      val key = splitPoints(i)
      linf = math.max(linf, math.abs(reference.getRank(key) - comparison.getRank(key)))
      i += 1
    }
    linf
  }

  private def denoisePmfs(referencePmf: Array[Double],
                          comparisonPmf: Array[Double],
                          noiseFloor: Double): (Array[Double], Array[Double]) = {
    val cleanedReference = Array.ofDim[Double](referencePmf.length)
    val cleanedComparison = Array.ofDim[Double](comparisonPmf.length)
    var i = 0
    while (i < referencePmf.length) {
      val reference = sanitizeProbability(referencePmf(i))
      val comparison = sanitizeProbability(comparisonPmf(i))
      if (math.max(reference, comparison) >= noiseFloor) {
        cleanedReference.update(i, reference)
        cleanedComparison.update(i, comparison)
      }
      i += 1
    }
    (normalizePmf(cleanedReference), normalizePmf(cleanedComparison))
  }

  private def sanitizeProbability(value: Double): Double =
    if (value.isNaN || value.isInfinite || value < 0.0) 0.0 else value

  private def normalizePmf(pmf: Array[Double]): Array[Double] = {
    val sum = pmf.sum
    if (sum > 0.0) pmf.map(_ / sum) else pmf
  }

  def percentileDistance(a: Array[Double], b: Array[Double], metric: DriftMetric, debug: Boolean = false): Double = {
    val breaks = (a ++ b).sorted.distinct
    val aProjected = AssignIntervals.on(a, breaks)
    val bProjected = AssignIntervals.on(b, breaks)

    val aNormalized = normalize(aProjected)
    val bNormalized = normalize(bProjected)

    val func = termFunc(metric)

    var i = 0
    var result = 0.0

    while (i < aNormalized.length) {
      val ai = aNormalized(i)
      val bi = bNormalized(i)
      val delta = func(ai, bi)

      result += delta
      i += 1
    }

    if (debug) {
      def printArr(arr: Array[Double]): String =
        arr.map(v => f"$v%.3f").mkString(", ")
      println(f"""
           |aProjected : ${printArr(aProjected)}
           |bProjected : ${printArr(bProjected)}
           |aNormalized: ${printArr(aNormalized)}
           |bNormalized: ${printArr(bNormalized)}
           |result     : $result%.4f
           |""".stripMargin)
    }
    result
  }

  // java map is what thrift produces upon deserialization
  type Histogram = java.util.Map[String, java.lang.Long]
  def histogramDistance(a: Histogram, b: Histogram, metric: DriftMetric): Double = {

    @inline def sumValues(h: Histogram): Double = {
      var result = 0.0
      val it = h.entrySet().iterator()
      while (it.hasNext) {
        result += it.next().getValue
      }
      result
    }
    val aSum = sumValues(a)
    val bSum = sumValues(b)

    val aIt = a.entrySet().iterator()
    var result = 0.0
    val func = termFunc(metric)
    while (aIt.hasNext) {
      val entry = aIt.next()
      val key = entry.getKey
      val aVal = entry.getValue.toDouble
      val bValOpt: java.lang.Long = b.get(key)
      val bVal: Double = if (bValOpt == null) 0.0 else bValOpt.toDouble
      val term = func(aVal / aSum, bVal / bSum)
      result += term
    }

    val bIt = b.entrySet().iterator()
    while (bIt.hasNext) {
      val entry = bIt.next()
      val key = entry.getKey
      val bVal = entry.getValue.toDouble
      val aValOpt = a.get(key)
      if (aValOpt == null) {
        val term = func(0.0, bVal / bSum)
        result += term
      }
    }

    result
  }

  @inline
  private def normalize(arr: Array[Double]): Array[Double] = {
    // TODO-OPTIMIZATION: normalize in place instead if this is a hotspot
    val result = Array.ofDim[Double](arr.length)
    val sum = arr.sum
    var i = 0
    while (i < arr.length) {
      result.update(i, arr(i) / sum)
      i += 1
    }
    result
  }

  @inline
  private def klDivergenceTerm(a: Double, b: Double): Double = {
    if (a > 0 && b > 0) a * math.log(a / b) else 0
  }

  @inline
  private def jsdTerm(a: Double, b: Double): Double = {
    val m = (a + b) * 0.5
    (klDivergenceTerm(a, m) + klDivergenceTerm(b, m)) * 0.5
  }

  @inline
  private def hellingerTerm(a: Double, b: Double): Double = {
    pow(sqrt(a) - sqrt(b), 2) * 0.5
  }

  @inline
  private def psiTerm(a: Double, b: Double): Double = {
    val aFixed = if (a == 0.0) 1e-5 else a
    val bFixed = if (b == 0.0) 1e-5 else b
    (bFixed - aFixed) * log(bFixed / aFixed)
  }

  @inline
  private def termFunc(d: DriftMetric): (Double, Double) => Double =
    d match {
      case DriftMetric.PSI            => psiTerm
      case DriftMetric.HELLINGER      => hellingerTerm
      case DriftMetric.JENSEN_SHANNON => jsdTerm
    }

  case class Thresholds(moderate: Double, severe: Double) {
    def str(driftScore: Double): String = {
      if (driftScore < moderate) "LOW"
      else if (driftScore < severe) "MODERATE"
      else "SEVERE"
    }
  }

  @inline
  def thresholds(d: DriftMetric): Thresholds =
    d match {
      case DriftMetric.JENSEN_SHANNON => Thresholds(0.05, 0.15)
      case DriftMetric.HELLINGER      => Thresholds(0.05, 0.15)
      case DriftMetric.PSI            => Thresholds(0.1, 0.2)
    }
}

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.perf;

import java.util.SplittableRandom;
import java.util.function.LongSupplier;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;


/**
 * Benchmark for SamplingStatsCollector.
 */
@State(Scope.Benchmark)
public class BenchmarkStatsCollector {
  private static final int NUM_DOCS = 10_000;

  // https://en.wikipedia.org/wiki/Great_Pyramid_of_Giza#Preparation_of_the_site
  private static final String[] WORDS =
      ("1,Phineas,Bergstram,pbergstram0@seesaaqlmanwo12r.net,Male,168.3.44.174,1,9/4/2022 00:30:44,Honorable,1,196"
          + ".181.88.228,"
          + "8676.67,400,\"Lorem ipsum dolor sit amet, consectetuer adipiscing elit. Proin interdum mauris non ligula"
          + " pellentesque ultrices.\",\"[{},{}]\",\"[{},{},{},{},{}]\",https://j2malmn"
          + ".com/mauris/vulputate/elementum/nullam.aspx?adipiscing=elit&molestie=proin&hendrerit=interdum&at=mauris"
          + "&vulputate=non&vitae=ligula&nisl=pellentesque&aenean=ultrices&lectus=phasellus&pellentesque=id&eget"
          + "=sapien&nunc=in&donec=sapien&quis=iaculis&orci=congue&eget=vivamus&orci=metus&vehicula=arcu&condimentum"
          + "=nulla&duis=ac&aliquam=enim&convallis=in&nunc=tempor&proin=turpis&at=nec&turpis=euismod&a=scelerisque"
          + "&pede=quam&posuere=turpis&nonummy=adipiscing&integer=lorem&non=vitae&velit=mattis&donec=nibh&diam=ligula"
          + "&neque=nec&vestibulum=sem&eget=duis&vulputate=aliquam,desktop,\"Mozilla/5.0 (X11; Linux i686) "
          + "AppleWebKit/535.11 (KHTML, like Gecko) Ubuntu/11.10 Chromium/17.0.963.65 Chrome/17.0.963.65 Safari/535"
          + ".11\",Windows,13.8811133,123.6886104,41.112.56.213,Rev,inactive,application,493.12,Nullam porttitor "
          + "lacus at turpis. Donec posuere metus vitae ipsum. Aliquam non mauris. Morbi non lectus. Aliquam sit amet"
          + " diam in magna bibendum imperdiet.,\"Phasellus in felis. Donec semper sapien a libero. Nam dui.\n"
          + "Proin leo odio, porttitor id, consequat in, consequat ut, nulla. Sed accumsan felis. Ut at dolor quis "
          + "odio consequat varius.\",Mr,\"\",213.72,\"Quisque erat eros, viverra eget, congue eget, semper rutrum, "
          + "nulla. Nunc purus. Phasellus in felis.\",\"In sagittis dui vel nisl. Duis ac nibh. Fusce lacus purus, "
          + "aliquet at, feugiat non, pretium quis, lectus.\n"
          + "Suspendisse potenti. In eleifend quam a odio. In hac habitasse platea dictumst.\",4759.07,21.06,515.11\n"
          + "2,Nikola,Van der Hoven,nvanderhoven1@kqalfdf.com,Male,35.22.223.15,2,1/14/2022 14:22:07,Mrs,2,172"
          + ".105.101.235,3624.95,200,\"Nulla tempus. Vivamus in felis eu sapien cursus vestibulum. Proin eu mi. "
          + "Nulla ac enim. In tempor, turpis nec euismod scelerisque, quam turpis adipiscing lorem, vitae mattis "
          + "nibh ligula nec sem. Duis aliquam convallis nunc. Proin at turpis a pede posuere nonummy. Integer non "
          + "velit. Donec diam neque, vestibulum eget, vulputate ut, ultrices vel, augue. Vestibulum ante ipsum "
          + "primis in faucibus orci luctus et ultrices posuere cubilia Curae; Donec pharetra, magna vestibulum "
          + "aliquet ultrices, erat tortor sollicitudin mi, sit amet lobortis sapien sapien non mi.\",[],\"[{},{},{},"
          + "{},{}]\",https://qkdnaj3ksosdk.com/accumsan/tortor/quis/turpis/sed/ante/vivamus"
          + ".png?velit=in&eu=congue&est=etiam&congue=justo&elementum=etiam&in=pretium&hac=iaculis&habitasse=justo"
          + "&platea=in&dictumst=hac&morbi=habitasse&vestibulum=platea&velit=dictumst&id=etiam&pretium=faucibus"
          + "&iaculis=cursus&diam=urna&erat=ut&fermentum=tellus&justo=nulla&nec=ut&condimentum=erat&neque=id&sapien"
          + "=mauris&placerat=vulputate&ante=elementum&nulla=nullam&justo=varius&aliquam=nulla&quis=facilisi&turpis"
          + "=cras&eget=non&elit=velit&sodales=nec&scelerisque=nisi&mauris=vulputate&sit=nonummy,tablet,\"Mozilla/5.0"
          + " ArchLinux (X11; U; Linux x86_64; en-US) AppleWebKit/534.30 (KHTML, like Gecko) Chrome/12.0.742.100\","
          + "iOS,44.7984547,14.8829285,14.180.147.23,Mrs,inactive,web,4583.79,\"Nullam sit amet turpis elementum "
          + "ligula vehicula consequat. Morbi a ipsum. Integer a nibh. In quis justo. Maecenas rhoncus aliquam lacus."
          + " Morbi quis tortor id nulla ultrices aliquet. Maecenas leo odio, condimentum id, luctus nec, molestie "
          + "sed, justo. Pellentesque viverra pede ac diam.\",\"Cum sociis natoque penatibus et magnis dis parturient"
          + " montes, nascetur ridiculus mus. Vivamus vestibulum sagittis sapien. Cum sociis natoque penatibus et "
          + "porttitor lorem id ligula. Suspendisse ornare consequat lectus. In est risus, auctor sed, tristique in, "
          + "tempus sit amet, sem. Fusce consequat. Nulla nisl. Nunc nisl. Duis bibendum, felis sed interdum "
          + "venenatis, turpis enim blandit mi, in porttitor pede justo eu massa. Donec dapibus.\",\"Cras mi pede, "
          + "malesuada in, imperdiet et, commodo vulputate, justo. In blandit ultrices enim. Lorem ipsum dolor sit "
          + "amet, consectetuer adipiscing elit.\n"
          + "Proin interdum mauris non ligula pellentesque ultrices. Phasellus id sapien in sapien iaculis congue. "
          + "Vivamus metus arcu, adipiscing molestie, hendrerit at, vulputate vitae, nisl.\n"
          + "Aenean lectus. Pellentesque eget nunc. Donec quis orci eget orci vehicula condimentum.\",Ms,\"\",135.95,"
          + "Sed ante. Vivamus tortor. Duis mattis egestas metus.,\"Morbi porttitor lorem id ligula. Suspendisse "
          + "ornare consequat lectus. In est risus, auctor sed, tristique in, tempus sit amet, sem.\n").split(" ");

  public static void main(String[] args)
      throws RunnerException {
    Options opt =
        new OptionsBuilder().include(BenchmarkStatsCollector.class.getSimpleName()).warmupTime(TimeValue.seconds(5))
            .warmupIterations(1).measurementTime(TimeValue.seconds(10)).measurementIterations(1).forks(2).build();

    new Runner(opt).run();
  }
//
//  @Benchmark
//  @BenchmarkMode(Mode.AverageTime)
//  @OutputTimeUnit(TimeUnit.MILLISECONDS)
//  public void benchULLStatsCollectorPrecision12(MyState myState, Blackhole bh) {
//    SamplingStatsCollector samplingStatsCollector = new SamplingStatsCollector();
//    for (int i = 0; i < myState._strings.length; i++) {
//      samplingStatsCollector.aggregate(new String(myState._strings[i]));
//    }
//    bh.consume(samplingStatsCollector.getEstimatedCardinality());
//  }
//
//  @Benchmark
//  @BenchmarkMode(Mode.AverageTime)
//  @OutputTimeUnit(TimeUnit.MILLISECONDS)
//  public void probabilisticCardinalityAggregatorRound10WorstCase(MyState myState, Blackhole bh) {
//    bh.consume(getProbabilisticCardinality(myState._strings, 10));
//  }
//
//  private double getProbabilisticCardinality(String[] strings, int rounds) {
//    ProbabilisticCardinalityAggregator probabilisticCardinalityAggregator =
//        new ProbabilisticCardinalityAggregator(rounds);
//    for (String string : strings) {
//      probabilisticCardinalityAggregator.aggregate(string);
//    }
//    return probabilisticCardinalityAggregator.getEstimatedCardinality();
//  }
//


  @State(Scope.Benchmark)
  public static class MyState {
    private String[] _strings;

//    @Param({"100000", "1000000"})
    @Param({"100000"})
    int _records;

    @Param({"UNIFORM(1,1000)", "EXP(0.001)"})
    String _distribution;

    @Setup(Level.Trial)
    public void doSetup() {
      SplittableRandom random = new SplittableRandom();
      LongSupplier supplier = Distribution.createLongSupplier(42, _distribution);
      _strings = new String[_records];
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < _records; i++) {
        int words = (int) supplier.getAsLong();
        for (int j = 0; j < words; j++) {
          sb.append(WORDS[random.nextInt(WORDS.length)]);
        }
        _strings[i] = sb.toString();
        sb.setLength(0);
      }
    }
  }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.twill.example.yarn;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multiset;
import com.google.common.util.concurrent.Futures;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.ClassAcceptor;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillContext;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.api.logging.PrinterLogHandler;
import org.apache.twill.yarn.YarnTwillRunnerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Hello World example using twill-yarn to run a TwillApplication over YARN.
 */
public class HelloWorld {
  public static final Logger LOG = LoggerFactory.getLogger(HelloWorld.class);

  /**
   * Hello World runnable that is provided to TwillRunnerService to be run.
   */
  private static class HelloWorldRunnable extends AbstractTwillRunnable {


    //private String time = 

    /*
    public HelloWorldRunnable(Map<String, String> stringStringMap) {
      super(stringStringMap);
      this.getContext().getArguments().
    }
    */

    private void runSystemCommand (String fsa,String finalWeight, String finalSymbol,String shard) throws IOException {

      LOG.info("Coming here ");

      File fsaFile = new File(fsa);
      File finalWeightFile = new File(finalWeight);
      File finalSymbolFile = new File(finalSymbol);
      FileSystem fs = FileSystem.get(new Configuration());
      FSDataOutputStream fsDataOutputStream = fs.create(new Path("/tmp/sam_wfst/shard_" + shard));


      Runtime rt = Runtime.getRuntime();


      try {
        String command = "wc -l " + fsaFile + " " + finalWeightFile + " " + finalSymbolFile;
        Process process = rt.exec(command);

        BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String line = null;
        while ((line = br.readLine()) != null) {
          LOG.info(line);
          fsDataOutputStream.writeUTF(line);
        }
        br = new BufferedReader(new InputStreamReader(process.getErrorStream()));
        line = null;
        while ((line = br.readLine()) != null) {
          LOG.info(line);
        }
      }
      finally {
        fsDataOutputStream.close();
        fs.close();
      }

    }

    private void printCurrentDirDetails(String dir){
      File currentDir = new File(dir);
      if(currentDir.isDirectory()){
        LOG.info("Yes this is a dir");
        File[] files = currentDir.listFiles();
        LOG.info("Found "+ files.length + "entries");
        for(File file:files){
          if(file.getName().startsWith("wfst_")){
            LOG.info("Size: "+file.length());
          }
          LOG.info(file.getName());
        }
      }
      else{
        LOG.info("No way .. just no way");
      }
    }


    @Override
    public void run() {

      try {

        String[] args = getContext().getArguments();

        String shard = args[0];
        //FileSystem fs = FileSystem.get(new Configuration());






        LOG.info("OH MY GOID");
        // LOG.info("Hello World. My "+ time +" distributed application. I can see this file " + filePath);


        runSystemCommand("finalFSA.txt","finalWeight.txt","finalSymbol.txt",shard);

      } catch (IOException e) {
        //
        LOG.info("Error running command");
      }
    }

    @Override
    public void stop() {
    }
  }



  public static void main(String[] args) throws IOException {
    if (args.length < 1) {
      System.err.println("Arguments format: <host:port of zookeeper server>");
      System.exit(1);
    }

    String zkStr = args[0];
    String shard = args[1];
    YarnConfiguration yarnConfiguration = new YarnConfiguration();
    final TwillRunnerService twillRunner =
      new YarnTwillRunnerService(
        yarnConfiguration, zkStr);
    twillRunner.start();

    String yarnClasspath =
      yarnConfiguration.get(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                            Joiner.on(",").join(YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH));
    List<String> applicationClassPaths = Lists.newArrayList();
    Iterables.addAll(applicationClassPaths, Splitter.on(",").split(yarnClasspath));



    final TwillController controller =
    //        twillRunner.prepare(new HelloWorldRunnable())
     twillRunner.prepare(new HelloWorldApplication(new YarnConfiguration(),shard))
        .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out, true)))
        .withApplicationClassPaths(applicationClassPaths)
        .withBundlerClassAcceptor(new HadoopClassExcluder())
        .withArguments("WFST",shard)
        .start();

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          Futures.getUnchecked(controller.terminate());
        } finally {
          twillRunner.stop();
        }
      }
    });

    try {
      controller.awaitTerminated();
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
  }

  static class HadoopClassExcluder extends ClassAcceptor {
    @Override
    public boolean accept(String className, URL classUrl, URL classPathUrl) {
      // exclude hadoop but not hbase package
      return !(className.startsWith("org.apache.hadoop") && !className.startsWith("org.apache.hadoop.hbase"));
    }
  }

  public static class HelloWorldApplication implements TwillApplication {

    Configuration conf;
    FileSystem fs;
    private String shard;
    public HelloWorldApplication(Configuration conf, String shard) throws IOException {
      this.conf = conf;
      fs = FileSystem.get(this.conf);
      this.shard = shard;
    }

    private Map<String,String> argMap() {
      Map<String, String> map= new HashMap<>();
      map.put("Time","third");
      return map;
    }




    @Override
    public TwillSpecification configure() {
      String pathPrefix = "/home/buildbot/wfst/shard_";

      try {
        return TwillSpecification.Builder.with()
                .setName("Twilltest")
                .withRunnable()
                .add("WFST", new HelloWorldRunnable(),ResourceSpecification.Builder.with()
                        .setVirtualCores(2)
                        .setMemory(1, ResourceSpecification.SizeUnit.GIGA)
                        //.setInstances(5)
                         .build()).withLocalFiles()
                .add("finalFSA.txt", new File(pathPrefix +shard+ "/finalFSA.txt"))
                .add("finalSymbol.txt", new File( pathPrefix + shard+ "/finalSymbol.txt"))
                .add("finalWeight.txt", new File(pathPrefix + shard+ "/finalWeight.txt"))
                .apply()
                /*.add("WFST_B",new HelloWorldRunnable(conf),ResourceSpecification.Builder.with()
                        .setVirtualCores(2)
                        .setMemory(1, ResourceSpecification.SizeUnit.GIGA)
                        //.setInstances(5)
                        .build()).withLocalFiles()
                .add("test_file",fs.resolvePath(new Path("/user/samprince_william/test.txt")).toUri())
                .add("wfst_b", new File("wfst/wfst_b"),"*")
                .apply()
                //.add("Helloworld 2",new HelloWorldRunnable()).noLocalFiles()*/
                .anyOrder()
                .build();
      }
      catch (Exception e){
        e.printStackTrace();
        return null;
      }
    }

   /* @Override
    public TwillSpecification configure()  {
      try {
        return TwillSpecification.Builder.with().setName("Twill Test")
                .withRunnable()
                .add("Hello World",new HelloWorldRunnable(), ResourceSpecification.Builder.with()
                        .setVirtualCores(2)
                        .setMemory(100, ResourceSpecification.SizeUnit.MEGA)
                        .setInstances(5).build()).withLocalFiles()
                .add("test_file",fs.resolvePath(new Path("/user/samprince_william/test.txt")).toUri())

                .apply()
                .anyOrder()
                .build();
      } catch (IOException e) {
        e.printStackTrace();
        return null;
      }

    }
    */

  }
}

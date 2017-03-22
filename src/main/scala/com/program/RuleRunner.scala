package com.program

import java.io.FileInputStream
import com.model.Temperature
import org.drools.KnowledgeBase
import org.drools.KnowledgeBaseFactory
import org.drools.builder.KnowledgeBuilder
import org.drools.builder.KnowledgeBuilderError
import org.drools.builder.KnowledgeBuilderErrors
import org.drools.builder.KnowledgeBuilderFactory
import org.drools.builder.ResourceType
import org.drools.builder._
import org.drools.logger.KnowledgeRuntimeLogger
import org.drools.logger.KnowledgeRuntimeLoggerFactory
import org.drools.io.ResourceFactory
import org.drools.runtime.StatefulKnowledgeSession
import org.drools.runtime._
import org.drools.event._
import org.drools.event.KnowledgeRuntimeEventManager

import org.apache.spark._
import org.apache.spark.rdd.RDD

object RuleRunner {

  def main(args : Array[String]):Unit ={
    
    println("Rule Engine Test in Single Mode")
    var ksession : StatefulKnowledgeSession = GetKnowledgeSession()
    println("Creating Knowledge Session")
    println("Creating and insertng Temperature")
    val temp1 = new Temperature(value=80.0)
    val temp2 = new Temperature(value=20.0)
    ksession.insert(temp1)
    ksession.insert(temp2)
    println("Firing all rules")
    ksession.fireAllRules()
    
    println("Rule Engine Test in Spark Mode")
    val sparkconf = new SparkConf().setAppName("Drools on Spark Demo")
    val sc = new SparkContext(sparkconf)
    val data = "100,20,34,567,98"
    println("  [Debug] Begin Execution!")
    val pairRDD = sc.parallelize(data.split(",").map(p => p.toDouble))
		    .mapPartitions{ partition => {
				      val ksession:StatefulKnowledgeSession = GetKnowledgeSession()
                                      val CountTemperatue = partition.map(p => {
						println("  [Debug] Begin Dealing the element: "+p.toString());
				                val temp = new Temperature(value=p);
						ksession.insert(temp);
                                                ksession.fireAllRules();
						println("  [Debug] Finish Dealing the element: "+p.toString());
                                                val result = temp.getStatus;
						(result,1)})
    				      println("  [Debug] Finish the data partition!");
				      CountTemperatue
                                      }
                                  }
                    .reduceByKey(_+_)
                    .foreach{p => println("  [Debug] Result: " + p._1.toString + ": Number :"+ p._2.toString+ "\n")}
  }

  def GetKnowledgeSession() : StatefulKnowledgeSession = {
    val config:KnowledgeBuilderConfiguration = KnowledgeBuilderFactory.newKnowledgeBuilderConfiguration()
    config.setProperty("drools.dialect.mvel.strict", "false")
    var kbuilder : KnowledgeBuilder  = KnowledgeBuilderFactory.newKnowledgeBuilder(config)
    kbuilder.add(ResourceFactory.newFileResource("/home/u928/lsj/DroolsSpark/src/main/scala/com/rules/test.drl"), ResourceType.DRL)
    println(kbuilder.getErrors().toString())
    var kbase : KnowledgeBase = KnowledgeBaseFactory.newKnowledgeBase()
    kbase.addKnowledgePackages(kbuilder.getKnowledgePackages())
    var ksession : StatefulKnowledgeSession = kbase.newStatefulKnowledgeSession()
    var logger : KnowledgeRuntimeLogger = KnowledgeRuntimeLoggerFactory.newFileLogger(ksession,"/root/DroolsSpark/drools.log")
    ksession
  }
}


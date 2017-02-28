package com.test.rules

import java.io.FileInputStream
import com.test.model.weather.Temperature
import org.drools.KnowledgeBase
import org.drools.KnowledgeBaseFactory
import org.drools.builder.KnowledgeBuilder
import org.drools.builder.KnowledgeBuilderError
import org.drools.builder.KnowledgeBuilderErrors
import org.drools.builder.KnowledgeBuilderFactory
import org.drools.builder.ResourceType
import org.drools.logger.KnowledgeRuntimeLogger
import org.drools.logger.KnowledgeRuntimeLoggerFactory
import org.drools.io.ResourceFactory
import org.drools.runtime.StatefulKnowledgeSession

object RuleRunner {

	  def main(args : Array[String]) : Unit = {
		  
                  //val line = "100,20"
		  var ksession : StatefulKnowledgeSession = GetKnowledgeSession()
                  //val pairRDD = line.split(",").map(p => p.toInt).map(p => new Temperature{ def value = p}).foreach(p => ksession.insert(p))

                  println("Creating Knowledge Session")
		  
		  
		  println("Creating and insertng Temperature")
		  
		 
		  val shouldBeTooHot = new Temperature {
			  def value = 100 
		  }
		  
		  val shouldBeTooCold = new Temperature {
			  def value = 20
		  }
		  
		  ksession.insert(shouldBeTooHot)
		  ksession.insert(shouldBeTooCold)
		  
		  println("Firing all rules")
		  
		  ksession.fireAllRules()
	  }
		  
	  def GetKnowledgeSession() : StatefulKnowledgeSession = {
		  var kbuilder : KnowledgeBuilder  = KnowledgeBuilderFactory.newKnowledgeBuilder()
                  var fis : FileInputStream = new FileInputStream("/root/DroolsSpark/WeatherRules.drl")
		  kbuilder.add(ResourceFactory.newInputStreamResource(fis), ResourceType.DRL)
		  var kbase : KnowledgeBase = KnowledgeBaseFactory.newKnowledgeBase()
		  kbase.addKnowledgePackages(kbuilder.getKnowledgePackages())
		  var ksession : StatefulKnowledgeSession = kbase.newStatefulKnowledgeSession()
		  var logger : KnowledgeRuntimeLogger = KnowledgeRuntimeLoggerFactory.newFileLogger(ksession,"/root/DroolsSpark/drools.log")
		  ksession
	  }	
}
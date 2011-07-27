package org.apache.solr.handler.dataimport.scheduler;

import java.util.Calendar;
import java.util.Date;
import java.util.Timer;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApplicationListener implements ServletContextListener {

        private static final Logger logger = LoggerFactory.getLogger(ApplicationListener.class);
        
        @Override
        public void contextDestroyed(ServletContextEvent servletContextEvent) {
                ServletContext servletContext = servletContextEvent.getServletContext();

                // get our timer from the context
                Timer timer = (Timer)servletContext.getAttribute("timer");

                // cancel all active tasks in the timers queue
                if (timer != null)
                        timer.cancel();

                // remove the timer from the context
                servletContext.removeAttribute("timer");

        }

        @Override
        public void contextInitialized(ServletContextEvent servletContextEvent) {
                ServletContext servletContext = servletContextEvent.getServletContext();
                try{
                       // create the timer and timer task objects
                        Timer timer = new Timer();
                        HTTPPostScheduler task = new HTTPPostScheduler(servletContext.getServletContextName());
                        
                        // get our interval from HTTPPostScheduler
                        int interval = task.getIntervalInt();
                        
                        // get a calendar to set the start time (first run)
                        Calendar calendar = Calendar.getInstance();
                        
                        // set the first run to now + interval (to avoid fireing while the app/server is starting)
                        calendar.add(Calendar.MINUTE, interval);
                        Date startTime = calendar.getTime();
                        
                        // schedule the task
                        //timer.scheduleAtFixedRate(task, startTime, 1000 * 60 * interval);
                        timer.scheduleAtFixedRate(task, startTime, 1000 * interval);

                        // save the timer in context
                        servletContext.setAttribute("timer", timer);
                        
                } catch (Exception e) {
                        if(e.getMessage().endsWith("disabled")){
                                logger.info("Schedule disabled");
                        }else{
                                logger.error("Problem initializing the scheduled task: ", e);   
                        }                       
                }
        }

}
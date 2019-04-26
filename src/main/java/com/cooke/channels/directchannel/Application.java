package com.cooke.channels.directchannel;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.MessageRejectedException;
import org.springframework.integration.annotation.Role;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.support.leader.LockRegistryLeaderInitiator;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;

import java.time.ZonedDateTime;

import static java.lang.String.format;
import static java.lang.Thread.sleep;
import static org.springframework.integration.support.MessageBuilder.withPayload;

/**
 * Sends and receives text messages from a channel
 */
@SpringBootApplication
public class Application {
    
    @Autowired
    private DirectChannel directChannel;

    @Autowired
    private LockRegistryLeaderInitiator lockRegistryLeaderInitiator;
      
    @Bean
    public DirectChannel directChannel() {
        return new DirectChannel();
    }

    @Bean
    @ServiceActivator(inputChannel = "directChannel", autoStartup="false")
    @Role("cluster")
    public MyMessageHandler handlerOne() {

        return new MyMessageHandler("Handler One");
    }

    @Bean
    @ServiceActivator(inputChannel = "directChannel", autoStartup="false")
    @Role("cluster")
    public MyMessageHandler handlerTwo() {
        return new MyMessageHandler("Handler Two");
    }

    @Bean
    @ServiceActivator(inputChannel = "directChannel", autoStartup="false")
    @Role("cluster")
    public MyMessageHandler handlerThree() {
        return new MyMessageHandler("Handler Three");
    }
    
    @Bean
    public CommandLineRunner commandLineRunner(final ApplicationContext ctx) {
        return args -> {
            while(true) {
                directChannel.send(withPayload("message from space at " + ZonedDateTime.now()).build());
                sleep(500);
            }
	    };
    }

    public static void main(final String[] args) {
        SpringApplication.run(Application.class, args);
    }

    public static class MyMessageHandler implements MessageHandler {

        private final String myName;

        public MyMessageHandler(final String myName) {
            this.myName = myName;
        }

        @Override
        public void handleMessage(final Message<?> message) throws MessagingException {
            final Object payload = message.getPayload();

            if (payload instanceof String) {
                print((String) payload);
            } else {
                throw new MessageRejectedException(message, "Unknown data type has been received.");
            }
        }

        void print(final String message) {
            System.out.println(format("%s #%s Received message: %s ", myName,  System.identityHashCode(this),  message));
        }
    }
}

package br.com.pedrosa.producer;

import br.com.pedrosa.common.KafkaConfigProperties;
import br.com.pedrosa.events.BookingPaymentEvent;
import br.com.pedrosa.events.SeatReservedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class PaymentEventsProducer {

    private final KafkaTemplate<String,Object> template;

    public void publishPaymentSuccessEvent(SeatReservedEvent event){
        log.info("Publishing payment success event ...");
        BookingPaymentEvent paymentEvent=new BookingPaymentEvent(event.bookingId(),true,event.amount());
        template
                .send(KafkaConfigProperties.PAYMENT_EVENTS_TOPIC, event.bookingId(),paymentEvent);

    }

    public void publishPaymentFailureEvent(SeatReservedEvent event){
        log.info("Publishing payment failure event ...");
        BookingPaymentEvent paymentEvent=new BookingPaymentEvent(event.bookingId(),false,event.amount());
        template
                .send(KafkaConfigProperties.PAYMENT_EVENTS_TOPIC, event.bookingId(),paymentEvent);

    }
}

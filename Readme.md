
Multi-threading is used in our order service and order consumer service to make things faster. Each service shares a consumer that delivers messages (topics) across the system. When the checkout function is called, it uses the shared consumer to send topics with a unique ID, so the right receiver knows which results are intended for them. By using multi-threading, the function doesn't have to wait for results before sending another topic, speeding up the process. We can have multiple order services and order consumer services based on the number of calls. However, we didn't implement multi-threading for payment and stock processes because it would introduce consistency errors and unnecessary overhead. Even if we used "thread locks" and ensured redis transaction pipeline, it would cause too many errors and slow things down, which isn't what we want right now.





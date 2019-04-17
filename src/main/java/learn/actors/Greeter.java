package learn.actors;

/**
 * Created by minfin data on 2019-04-15
 */
import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

// 继承Actor抽象类
public class Greeter extends AbstractActor {
    static public Props props(String message, ActorRef printerActor) {
        // -> lambda表达式，java8新特性
        // 比如：
        // () -> 5      不需要参数,返回值为 5
        // x -> 2 * x   接收一个参数(数字类型),返回其2倍的值
        // (int x, int y) -> x + y   接收2个int型整数,返回他们的和
        // (String s) -> System.out.print(s) 接受一个 string 对象,并在控制台打印,不返回任何值(看起来像是返回void)

        return Props.create(Greeter.class, () -> new Greeter(message, printerActor));
    }

    static public class WhoToGreet {
        public final String who;

        public WhoToGreet(String who) {
            this.who = who;
        }
    }

    static public class Greet {
        public Greet() {
        }
    }

    private final String message;
    private final ActorRef printerActor;
    private String greeting = "";

    public Greeter(String message, ActorRef printerActor) {
        this.message = message;
        this.printerActor = printerActor;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(WhoToGreet.class, wtg -> {
                    this.greeting = message + ", " + wtg.who;
                }).match(Greet.class, x -> {
                    printerActor.tell(new Printer.Greeting(greeting), getSelf());
                }).build();
    }
}
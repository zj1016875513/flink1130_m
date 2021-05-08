/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/8 11:03
 */
public class Demo {
    public static void main(String[] args) {
        /*f(new A() {
            @Override
            public int foo1() {
                System.out.println("foo1...");
                return 10;
            }
        });*/
        
        
    /*f(() -> {
            System.out.println("abc...");
            return 10;
        });*/
        
        f(() -> 10);
    }
    
    public static void f(A a) {
        int i = a.foo1();
        System.out.println(i);
        //        a.foo2();
    }
}

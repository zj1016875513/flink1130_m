/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/8 11:01
 */
public interface A {
    int a = 10;
    
    int foo1();
//    void foo3();
    
    default void foo2() {
       
        System.out.println("xxxx");
    }
}

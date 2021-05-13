/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/13 9:44
 */
public class FF {
    public static void main(String[] args) {
        byte i = -128;
        
        i -= 1;
        System.out.println(i);
    }
}

class Dog<T> {
    T t;
    
    public void speak(T t) {
        T a;
    }
    
    public <M> M foo(M m) {
        
        return m;
        
    }
}


/*
泛型: 类型的参数化
定义在类上
    在这个类的内部任何地方都可以使用这个泛型
定义在方法上
    这个泛型只能在方法内部使用
 */

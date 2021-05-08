/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/8 11:35
 */
public class Demo2 {
    public static void main(String[] args) {
        /*f(new C(){
            @Override
            public String foo(User user) {
                return user.getName();
            }
        });*/
        //f(user -> user.getName());
    
       // f(User::getName);  // 方法引用
        
        f1(new D() {
            @Override
            public User foo() {
                return new User();
            }
        });
    
        f1(() -> new User());
        f1(User::new);
    }
    
    private static void f1(D d){
        User user = d.foo();
        System.out.println(user.getName());
    }
    
    public static void f(C c){
        User user = new User();
        user.setName("zs");
        String r = c.foo(user);
        System.out.println(r);
    }
    
}

interface D{
    User foo();
}

interface C {
    String foo(User user);
}

class User {
    private String name;
    
    public String getName() {
        return name;
    }
    
    public void setName(String name) {
        this.name = name;
    }
    
    @Override
    public String toString() {
        return "User{" +
            "name='" + name + '\'' +
            '}';
    }
}
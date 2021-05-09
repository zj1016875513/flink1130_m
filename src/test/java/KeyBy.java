import org.apache.flink.util.MathUtils;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/5/9 15:54
 */
public class KeyBy {
    public static void main(String[] args) {
//        Integer a = 0;
//        Integer b = 1;
        String a = "偶数";
        String b = "奇数";
        System.out.println(MathUtils.murmurHash(a.hashCode()) % 128);
        System.out.println(MathUtils.murmurHash(b.hashCode()) % 128);
    }
}

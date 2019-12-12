package javakafka.io.kafka.message;
/**
 * @author tf
 * @version 创建时间：2019年1月10日 下午5:56:46
 * @ClassName 类名称
 */
public class FileChannelTest {

	static FileChannelTest t=new FileChannelTest();
	public static void main(String[] args) {

		try {
			beforeMethod(1);
			t.MethodTest(1);

		}catch (Exception ex){

		}
		t.MethodTest(1);
	}

	private static void beforeMethod(int userId) {
	}


	public void MethodTest(int userId){
		System.out.println("Hello~~");
	}

	private void handleMethodException(Exception e) {
	}

	private void afterMethod(int userId) {
	}

}

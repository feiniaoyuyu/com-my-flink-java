package asynio;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.Scanner;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.DeferredGroupException;

public class Demo1 {
	private static final byte[] TABLE_NAME = null;
	private static final byte[] INFO_FAM = null;
	private static final byte[] PASSWORD_COL = null;

	public static void main(String[] args) throws Throwable {  
		   final HBaseClient client = new HBaseClient("localhost");  
		   Deferred<ArrayList<Boolean>> d = Deferred.group(doList(client));  
		   try {  
		       d.join();  
		   } catch (DeferredGroupException e) {  
		       LOG.info(e.getCause().getMessage());  
		   }  
		  
		   //线程阻塞直到shutdown完成  
		   client.shutdown().joinUninterruptibly();  
		 }  
		  
		static List<Deferred<Boolean>> doList(HBaseClient client)  
		     throws Throwable {  
		   final Scanner scanner = client.newScanner(TABLE_NAME);  
		   scanner.setFamily(INFO_FAM);  
		   scanner.setQualifier(PASSWORD_COL);  
		  
		   ArrayList<ArrayList<KeyValue>> rows = null;  
		   ArrayList<Deferred<Boolean>> workers  
		     = new ArrayList<Deferred<Boolean>>();  
		   //线程阻塞直到返回所有查询结果  
		   while ((rows = scanner.nextRows(1).joinUninterruptibly()) != null) {  
		     LOG.info("received a page of users.");  
		     for (ArrayList<KeyValue> row : rows) {  
		       KeyValue kv = row.get(0);  
		       byte[] expected = kv.value();  
		       String userId = new String(kv.key());  
		       PutRequest put = new PutRequest(  
		           TABLE_NAME, kv.key(), kv.family(),  
		           kv.qualifier(), mkNewPassword(expected));  
		       //任务链构建  
		       Deferred<Boolean> d = client.compareAndSet(put, expected)  
		         .addCallback(new InterpretResponse(userId))  
		         .addCallbacks(new ResultToMessage(), new FailureToMessage())  
		         .addCallback(new SendMessage());  
		       workers.add(d);  
		     }  
		   }  
		   return workers;  
		 }  
		    
		  private static byte[] mkNewPassword(byte[] expected) {
			return Bytes.toBytes( new String(expected) + "pwd");
		}

		//等待hbase操作结果返回response  
		  static final class InterpretResponse  
		     implements Callback<UpdateResult, Boolean> {  
		  
		   private String userId;  
		  
		   InterpretResponse(String userId) {  
		     this.userId = userId;  
		   }  
		  
		   public UpdateResult call(Boolean response) throws Exception {  
		     latency();  
		  
		     UpdateResult r = new UpdateResult();  
		     r.userId = this.userId;  
		     r.success = entropy(response);  
		     if (!r.success)  
		       throw new UpdateFailedException(r);  
		  
		     latency();  
		     return r;  
		   }   

		@Override  
		   public String toString() {  
		     return String.format("InterpretResponse<%s>", userId);  
		   }  
		 }  
		  
		  //密码更新成功  
		  static final class ResultToMessage  
		     implements Callback<String, UpdateResult> {  
		  
		   public String call(UpdateResult r) throws Exception {  
		     latency(); 
		     String fmt = "password change for user %s successful.";  
		     latency();  
		     return String.format(fmt, r.userId);  
		   }  
		   
		@Override  
		   public String toString() {  
		     return "ResultToMessage";  
		   }  
		 }  
		   
		 //密码更新失败  
		 static final class FailureToMessage  implements Callback<String, UpdateFailedException> {  
		  
		   public String call(UpdateFailedException e) throws Exception {  
		     latency();  
		     String fmt = "%s, your password is unchanged!";  
		     latency();  
		     return String.format(fmt, e.result.userId);  
		   }  
		  
		   @Override  
		   public String toString() {  
		     return "FailureToMessage";  
		   }  
		 }  
		   
		 ///发送信息  
		 static final class SendMessage  
		     implements Callback<Boolean, String> {  
		  
		   public Boolean call(String s) throws Exception {  
		     latency();  
		     if (entropy(null))  
		       throw new SendMessageFailedException();  
		     LOG.info(s);  
		     latency();  
		     return Boolean.TRUE;  
		   }  
		  
		   @Override  
		   public String toString() {  
		     return "SendMessage";  
		   }  
		 }  
		 
		 static class UpdateResult {

			public Boolean success;
			public String userId;
			 
		 }
		 static class LOG {
			 public static void info(String s) {
				
			}
		 }
		 private static void latency() {
			   
		 }
		 
		public static boolean entropy(Boolean object) {
 			return false;
		}

		static class UpdateFailedException extends Exception{
 
			public UpdateResult result;

			public UpdateFailedException(UpdateResult r) {
				
 			}
			private static final long serialVersionUID = 1L;
			 
		 }
		
		static class SendMessageFailedException extends Exception{
 
			private static final long serialVersionUID = 1L;
			
		}

}


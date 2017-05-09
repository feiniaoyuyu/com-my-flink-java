package org.com.pateo.flink.streaming;

import org.apache.flink.api.common.Plan;
import org.apache.flink.client.program.PackagedProgram;

public class ProgramPlan implements org.apache.flink.api.common.Program {
 
	private static final long serialVersionUID = 1L;

	@Override
	public Plan getPlan(String... args) {
		// TODO Auto-generated method stub
		
//		new PackagedProgram(jarFile, args);
		return null;
	}
	

}

package com.java.hdfs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//Mapper<(입력키<행번호> : 입력값<행의글자>) , (출력키<글자> : 출력값<1>)>
public class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

	// 출력 키 변수
	protected Text textKey = new Text();
	// 출력 값 변수
	protected IntWritable intValue = new IntWritable(1);
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		String[] values = value.toString().split(",");
		// 기존 값 1311826
		// values[0] = 항공사 년도
		// values[8] = 항공사 코드
		
		// 출력 키에 넣을 문자열 변수
		String strKey = values[8];
		
		if("1987".equals(values[0]) && !"NA".equals(strKey)) { // 1987년에 NA가 아닌 데이터만 읽는다!
			intValue = new IntWritable(1);
		} else {
			intValue = new IntWritable(0);
		}
		// 출력 키에 문자열 변수 적용
		textKey.set(strKey);
		// 전체 결과 출력하기
		context.write(textKey, intValue);
		
	}
	
}

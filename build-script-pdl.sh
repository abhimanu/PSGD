hadoop="hadoop-0.20.1-core.jar"

rm -rf classes/*

javac -classpath ${HADOOP_HOME}/$hadoop -d classes/ PSGDRandomSplitter.java PSGDSplitterMapper.java PSGDSplitterReducer.java FloatArray.java ReaderWriterClass.java Tensor.java DenseTensor.java Matrix.java 
jar -cvf PSGDRandomSplitter.jar -C classes/ ./

rm -rf classes/*

javac -classpath ${HADOOP_HOME}/$hadoop -d classes/ PSGDCombiner.java PSGDCombinerMapper.java PSGDCombinerReducer.java FloatArray.java ReaderWriterClass.java Tensor.java DenseTensor.java Matrix.java 
jar -cvf PSGDCombiner.jar -C classes/ ./

rm -rf classes/*

javac -classpath ${HADOOP_HOME}/$hadoop -d classes/ FrobeniusPSGD.java FloatArray.java ReaderWriterClass.java Tensor.java DenseTensor.java Matrix.java 
jar -cvf FrobeniusPSGD.jar -C classes/ ./

rm -rf classes/*

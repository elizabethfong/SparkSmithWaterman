# Smith-Waterman Algorithm for Spark  
**Insight Data Engineering, NYC, September - October 2015**  
Elizabeth Fong  

[Link to Demo](http://www.slideshare.net/ElizabethWaiMingFong/week-4-53376723)  

## Contents  
* [Background](https://github.com/elizabethfong/SparkSmithWaterman#background)  
* [Development Tools](https://github.com/elizabethfong/SparkSmithWaterman#development-tools) 
* [Data Source](https://github.com/elizabethfong/SparkSmithWaterman#data-source)  
* [Maven Dependencies](https://github.com/elizabethfong/SparkSmithWaterman#maven-dependencies)
* [Algorithm Pseudocode](https://github.com/elizabethfong/SparkSmithWaterman#algorithm-pseudocode)
   * Matrix Generation and Filling
   * Extracting Optimal Alignments from Completed Matrix
* [Parallelization](https://github.com/elizabethfong/SparkSmithWaterman#parallelization)
   * [Parallelize Reference Set](https://github.com/elizabethfong/SparkSmithWaterman#parallelize-reference-set)
      * Pseudocode - General Method
      * Pseudocode - Map Function
   * [Parallelize Algorithm](https://github.com/elizabethfong/SparkSmithWaterman#parallelize-algorithm)
      * Pseudocode - General Method
      * Pseudocode - Parallelized Smith-Waterman Algorithm

## Background
The Smith-Waterman genetic alignment algorithm is used as a base in many other algorithms, such as the widely-used BLAST algorithm. These algorithms are used to match an input, to a reference set of genomic sequences, to find sequences which best-match the input, and the locations in which the sequences align.  

The input may be in a variety of file formats, one of which is the FASTA format, where an input sequence is broken up into multiple smaller sequences, called reads. Each read is compared to the reference set and the results are combined to find the reference sequence with the best match.

## Development Tools  
* Hadoop 2.7.1
* Apache Spark 1.5.2
* Java 8  
* Apache Maven 3.3 (for dependencies)

## Data Source
**[National Center for Biotechnology Information](ftp://ftp.ncbi.nlm.nih.gov/refseq/release)**
* total number of files = **518**
* total number of sequences = **12,321,160**
* total number of base pairs = **26,623,169,330**
* mean (average) number of base pairs per sequence = **2,160**
* median number of base pairs per sequence = **1,609**

## Maven Dependencies
**Apache Maven 3.3**
* [Spark Project Core 2.10](http://mvnrepository.com/artifact/org.apache.spark/spark-core_2.10) version 1.4.1
* [Apache Hadoop Client](http://mvnrepository.com/artifact/org.apache.hadoop/hadoop-client/2.7.1) version 2.7.1
* Amazon AWS Java SDK
   * [AWS SDK for Java](http://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk) version 1.9.0
   * [Amazon S3](http://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk-s3) version 1.9.0
   * [DynamoDB](http://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk-dynamodb) version 1.9.0

## Algorithm Pseudocode
**Matrix Generation and Filling**   

    Given: String s1 with length m , String s2 with length n
    
        // initialize matrix, M
        
        // score cells in matrix
        for i=1 to m
            for j=1 to n
            
                // initialization: max is 0
                max = 0 
                
                // first comparison: west cell (deletion)
                score = M[i][j-1] + gapScore
                if( score > max )
                    max = score
                
                // second comparison: north cell (insertion)
                score = M[i-1][j] + gapScore
                if( score > max )
                    max = score
                
                // last comparison: north-west cell (alignment)
                base1 = s1[j-1]
                base2 = s2[i-1]
                
                if( base1 == base2 )              // match
                    alignmentScore = matchScore
                else                              // mismatch
                    alignmentScore = mismatchScore
                
                score = M[i-1][j-1] + alignmentScore
                if( score > max )
                    max = score
                
                // finished all comparisons
                M[i][j] = max
        
        // return completed matrix
        return M

**Extracting Optimal Alignments from Completed Matrix**

    Given: Matrix M, String s1 with length m , String s2 with length n
    
        // find cells with the maximum score
        startingCells = list of cells with maximum score
        
        // find optimal alignment for each starting cell
        optimalAlignments = list of optimal alignments, initially empty
        
        for( cell : startingCells )
            
            stack = initially empty Stack - keep track of cells in the alignment
            i,j = coordinates of cell
            
            // backtrack from starting cell to find optimal alignment
            while( cell.score > 0 )
                
                // alignment score - max score was from NW cell
                if( cell.score is an alignment score )
                    base1 = s1[j-1]
                    base2 = s2[i-1]
                    stack.push( {base1,base2} )
                    i = i-1
                    j = j-1
                
                // insertion score - max score was from north cell
                else if( cell.score is an insertion score )
                    base1 = GAP
                    base2 = s2[i-1]
                    stack.push( {base1,base2} )
                    i = i-1
                
                // deletion score - max score was from west cell
                else
                    base1 = s1[j-1]
                    base2 = GAP
                    stack.push( {base1,base2} )
                    j = j-1
                
            // completed alignments - pop stack to get optimal alignment
            for( bases : stack )
                a1 = a1 + bases[0]
                a2 = a2 + bases[1]
            
            optimalAlignments.add( {a1,a2} )
        
        // return optimal alignments
        return optimalAlignments
    

## Parallelization  
###Parallelize Reference Set  
* Faster
* Uses up a lot of memory

**Pseudocode - General Method** 

    Given: List of reference sequences, refs (has m sequences)  
    Given: List of reads of an input (has n reads)
        
        SparkContext sc = new SparkContext() 
        
        // parallelize and map reference sequences
        rdd = sc.parallelize( refs )
        pairRDD = rdd.map( mapFunction() )    // <K,V> = <score,optimalAlignments>
        
        // reduce - get sequence(s) with maximum score
        maxScore = pairRDD.maxKey()
        List optimalSequences = pairRDD.lookup(maxScore)
        
        // return
        return optimalSequences

**Pseudocode - Map Function**

    Given: List of reference sequences, refs (has m sequences)  
    Given: List of reads of an input (has n reads)
        
        // variables
        int totalScore = 0 
        List optimalAlignments = list of optimal sequences & their alignments, empty
        
        // add up score for each read and add optimal alignments to list
        for( read : input )
            
            // run Smith-Waterman algorithm (non-parallelized)
            result = smithWaterman( ref , read )
            
            totalScore = totalScore + result.score
            optimalAlignments.add( result.optimalAlignments )
        
        // return <K,V> = <score,optimalAlignments>
        return { totalScore , optimalAlignments }

###Parallelize Algorithm  
* Slower
* Uses less memory

**Pseudocode - General Method**

    Given: List of reference sequences, refs (has m sequences)  
    Given: List of reads of an input (has n reads)
        
        int max = 0
        List optSeqs = list of optimal sequences and their alignments, empty
        
        // get score for each reference sequence
        for( ref : refs )
            
            // variables
            int totalScore = 0 
            List opt = list of optimal sequences and their alignments, init empty
            
            // add up all scores for each read, add optimal alignments to list
            for( read : input )
            
                // run parallelized Smith-Waterman algorithm
                result = smithWaterman( ref , read )
                
                totalScore = totalScore + result.score
                opt.add( result.optimalAlignments )
            
            // update maximum score and optimal sequences
            if( totalScore > max )
                max = totalScore
                optSeqs = opt
            
            else if( totalScore == max )
                optSeqs.add( opt )
        
        // return
        return { max , optSeqs } 

**Pseudocode - Parallelized Smith-Waterman Algorithm**

    Given: String ref (length m), String read (length n) 
        
        // initialize matrix M (above)
        
        // MATRIX FILLING
        // treat each diagonal row of cells as a list
        List current = top left cell
        List children = next diagonal row
        List grandchildren = next diagonal row
        
        SparkContext sc = new SparkContext()
        
        // repeat for each list until matrix is filled
        while( list != null )
        
            // parallelize and map each cell in the current list
            // map function: calculate the cell score for each cell
            rdd = sc.parallelize( list )
            rdd = rdd.map( calculateCellScore() )
            
            // reduce step: each cell broadcast its score to S,E,SE cells
            // each cell broadcasts to at most 3 cells
            // S,E cells - in children list
            // SE cell - in grandchildren list
            List cells = rdd.collect()
            
            for( cell : cells )
                M[i][j] = cell.score
                cell.broadcastScore()
            
            // update lists
            list = children
            children = grandchildren
            grandchildren = getNextList()    // may be null
        
        
        // GET OPTIMAL ALIGNMENTS
        // get list of starting cells
        startingCells = list of cells with maximum score
        
        // parallelize and map each starting cell
        // map function: find each optimal alignment (above)
        rdd = sc.parallelize( startingCells )
        rdd = rdd.map( getOptimalAlignments() )
        
        // reduce step: add optimal alignments to a list
        List opt = rdd.reduce( addAlignmentsToList() )
        
        
        // RETURN: maximum score and optimal alignments
        return { M.maxScore , opt }
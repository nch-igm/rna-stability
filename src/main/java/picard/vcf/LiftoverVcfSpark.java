package picard.vcf;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.SystemUtils;

import htsjdk.samtools.SAMSequenceRecord;
import htsjdk.samtools.liftover.LiftOver;
import htsjdk.samtools.reference.ReferenceSequence;
import htsjdk.samtools.reference.ReferenceSequenceFileWalker;
import htsjdk.samtools.util.CloserUtil;
import htsjdk.samtools.util.Interval;
//import htsjdk.samtools.util.Log;
import htsjdk.samtools.util.StringUtil;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.VariantContextBuilder;
import htsjdk.variant.vcf.VCFEncoder;
import htsjdk.variant.vcf.VCFFileReader;
import htsjdk.variant.vcf.VCFHeader;

public class LiftoverVcfSpark extends LiftoverVcf {

//    private final Log log = Log.getInstance(LiftoverVcf.class);
    
    private LiftOver liftOver = null;
    private Map<String, ReferenceSequence> refSeqs = null;
    
	// Default to the normal Java IO temp directory
	File memFileDir = SystemUtils.getJavaIoTmpDir();   
	File tempFile = null;
	VCFEncoder encoder = null;
    
	
    public static void main(String args[]) {
    	LiftoverVcfSpark liftOverSpark = new LiftoverVcfSpark("/tmp/hg38ToHg19.over.chain","/tmp/hg19.fa");    	
    	liftOverSpark.initForSpark();
    	
    	String line = "chr1\t685851\t.\tG\tC\t.\t.\t.";    
    	String result = liftOverSpark.liftOverVcfLine(line);
    	System.out.println("result = " + result);
    	
    	String line2 = "chr1\t451361\t.\tG\tT\t.\t.\t.";
    	String result2 = liftOverSpark.liftOverVcfLine(line2);
    	System.out.println("result2 = " + result2);
    	
    	String result3 = liftOverSpark.liftOverPosition("1", 685851, "G", "C");
    	System.out.println("result3 = " + result3);
    	
    	String result4 = liftOverSpark.liftOverPosition("X", 685851, "G", "C");
    	System.out.println("result4 = " + result4);
    	
    	liftOverSpark.cleanup();
    }
    
    
	public LiftoverVcfSpark(String chainFile, String referenceSequenceFile) {
		super();
		
    	this.CHAIN = new File(chainFile);
    	this.REFERENCE_SEQUENCE = new File(referenceSequenceFile);		
    	this.WRITE_ORIGINAL_POSITION = true;
	}
	
	public void initForSpark() {
        liftOver = new LiftOver(CHAIN);

        //log.info("Loading up the target reference genome.");
        final ReferenceSequenceFileWalker walker = new ReferenceSequenceFileWalker(REFERENCE_SEQUENCE);
        refSeqs = new HashMap<>();
        for (final SAMSequenceRecord rec : walker.getSequenceDictionary().getSequences()) {
            refSeqs.put(rec.getSequenceName(), walker.get(rec.getSequenceIndex()));
        }
        CloserUtil.close(walker);
        //log.info("Finished loading the target reference genome.");
        
		if (SystemUtils.IS_OS_LINUX) {
			// Use the tmpfs on Linux
			memFileDir = new File("/dev/shm");
		} else if (SystemUtils.IS_OS_MAC_OSX) {
			// Create our own ram disk on the Mac
			// NOTE: You need to have created this yourself prior to running this application. To do that run these commands:
			// hdid -nomount ram://2048
			// diskutil eraseVolume HFS+ RAMDisk PATH_TO_DISK_CREATED_FROM_HDID
			memFileDir = new File("/Volumes/RAMDisk");
		}   
		
		try {
			tempFile = File.createTempFile("tempfile", ".tmp", memFileDir);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void cleanup() {
		tempFile.delete();
		encoder = null;
	}
	
	protected void writeVcfLineToMemFile(String line) {
		try {
//			tempFile = File.createTempFile("tempfile", ".tmp", memFileDir);			
			
			FileWriter fw = new FileWriter(tempFile);
		    BufferedWriter bw = new BufferedWriter(fw);
		    bw.write("##fileformat=VCFv4.1");
		    bw.newLine();
		    bw.write("#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO");
		    bw.newLine();
		    bw.write(line);
		    bw.newLine();
		    bw.close();			
		    fw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}		
	}
	
	public String liftOverPosition(String chromosome_id, int position, String ref, String alt) {
		StringBuffer line = new StringBuffer();
		
		// Picard wants the chromosome position denoted like chr1 instead of 1.
		if (!chromosome_id.startsWith("chr")) {
			line.append("chr");
		}
		
		line.append(chromosome_id);
		line.append('\t');
		line.append(position);
		line.append('\t');
		line.append(".");
		line.append('\t');
		line.append(ref);
		line.append('\t');
		line.append(alt);
		line.append('\t');
		line.append(".");
		line.append('\t');
		line.append(".");
		line.append('\t');
		line.append(".");
		
		return liftOverVcfLine(line.toString());
	}
	
	public String liftOverVcfLine(String line) {
		VariantContext returnValue = null;		

		writeVcfLineToMemFile(line);
		
		Map<Allele, Allele> reverseComplementAlleleMap = new HashMap<>(10);
		
		@SuppressWarnings("resource")
		VCFFileReader in = new VCFFileReader(tempFile, false);	
        VCFHeader inHeader = in.getFileHeader();

        if (encoder == null) {
        	encoder = new VCFEncoder(inHeader, true, true);
        }
	
	    for (final VariantContext ctx : in) {
	        final Interval source = new Interval(ctx.getContig(), ctx.getStart(), ctx.getEnd(), false, ctx.getContig() + ":" + ctx.getStart() + "-" + ctx.getEnd());
	        final Interval target = liftOver.liftOver(source, LIFTOVER_MIN_MATCH);
	
	        // target is null when there is no good liftover for the context. This happens either when it fall in a gap
	        // where there isn't a chain, or if a large enough proportion of it is diminished by the "deletion" at the
	        // end of each interval in a chain.
	        if (target == null) {
	            //rejectVariant(ctx, FILTER_NO_TARGET);
	        	returnValue = new VariantContextBuilder(ctx).filter(FILTER_NO_TARGET).make();
	            continue;
	        }
	
	        // the target is the lifted-over interval comprised of the start/stop of the variant context,
	        // if the sizes of target and ctx do not match, it means that the interval grew or shrank during
	        // liftover which must be due to straddling multiple intervals in the liftover chain.
	        // This would invalidate the indel as it isn't clear what the resulting alleles should be.
	        if (ctx.getReference().length() != target.length()){
	            //rejectVariant(ctx, FILTER_INDEL_STRADDLES_TWO_INTERVALS);
	        	returnValue = new VariantContextBuilder(ctx).filter(FILTER_INDEL_STRADDLES_TWO_INTERVALS).make();
	            continue;
	        }
	
	        final ReferenceSequence refSeq;
	
	        // if the target is null OR (the target is reverse complemented AND the variant is a non-biallelic indel or mixed), then we cannot lift it over
	        if (target.isNegativeStrand() && (ctx.isMixed() || ctx.isIndel() && !ctx.isBiallelic())) {
	        	returnValue = new VariantContextBuilder(ctx).filter(FILTER_CANNOT_LIFTOVER_INDEL).make();
	
	        } else if (!refSeqs.containsKey(target.getContig())) {
	        	returnValue = new VariantContextBuilder(ctx).filter(FILTER_NO_TARGET).make();
	
	            final String missingContigMessage = "Encountered a contig, " + target.getContig() + " that is not part of the target reference.";
	            if (WARN_ON_MISSING_CONTIG) {
	                //log.warn(missingContigMessage);
									System.out.println(missingContigMessage);
	            } else {
//	                log.error(missingContigMessage);
									System.out.println(missingContigMessage);
	                // GRANT_HACK What to do here?
	                //return EXIT_CODE_WHEN_CONTIG_NOT_IN_REFERENCE;
	            }
	        } else if (target.isNegativeStrand() && ctx.isIndel() && ctx.isBiallelic()) {
	            refSeq = refSeqs.get(target.getContig());
	            //flipping indels:
	
	            final VariantContext flippedIndel = flipIndel(ctx, liftOver, refSeq);
	            if (flippedIndel == null) {
	                throw new IllegalArgumentException("Unexpectedly found null VC. This should have not happened.");
	            } else {
	            	returnValue = tryToAddVariant(flippedIndel, refSeq, reverseComplementAlleleMap, ctx);
	            }
	        } else {
	            refSeq = refSeqs.get(target.getContig());
	            final VariantContext liftedVariant = liftSimpleVariant(ctx, target);
	
	            returnValue = tryToAddVariant(liftedVariant, refSeq, reverseComplementAlleleMap, ctx);
	        }
	    }	
	    
	    in.close();
	    //tempFile.delete();
	    		
	    return encoder.encode(returnValue);	    
	}
	
    private VariantContext tryToAddVariant(final VariantContext vc, final ReferenceSequence refSeq, final Map<Allele, Allele> alleleMap, final VariantContext source) {
    	VariantContext returnValue = null;
    	
    	
        final VariantContextBuilder builder = new VariantContextBuilder(vc);

        builder.genotypes(fixGenotypes(source.getGenotypes(), alleleMap));
        builder.filters(source.getFilters());
        builder.log10PError(source.getLog10PError());
        builder.attributes(source.getAttributes());
        builder.id(source.getID());

        if (WRITE_ORIGINAL_POSITION) {
            builder.attribute(ORIGINAL_CONTIG, source.getContig());
            builder.attribute(ORIGINAL_START, source.getStart());
        }

        // Check that the reference allele still agrees with the reference sequence
        boolean mismatchesReference = false;
        for (final Allele allele : builder.getAlleles()) {
            if (allele.isReference()) {
                final byte[] ref = refSeq.getBases();
                final String refString = StringUtil.bytesToString(ref, vc.getStart() - 1, vc.getEnd() - vc.getStart() + 1);

                if (!refString.equalsIgnoreCase(allele.getBaseString())) {
                    mismatchesReference = true;
                }
                break;
            }
        }

        if (mismatchesReference) {
        	returnValue = new VariantContextBuilder(source)
                    .filter(FILTER_MISMATCHING_REF_ALLELE)
                    .attribute(ATTEMPTED_LOCUS, String.format("%s:%d-%d",vc.getContig(),vc.getStart(),vc.getEnd()))
                    .make();
            //failedAlleleCheck++;
        } else {
            //sorter.add(builder.make());
        	returnValue = builder.make();
        }
        
        return returnValue;
    }	
}

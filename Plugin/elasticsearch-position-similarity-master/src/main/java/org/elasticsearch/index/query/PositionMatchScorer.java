/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.index.query;

import org.apache.logging.log4j.LogManager;
import org.apache.lucene.analysis.payloads.PayloadHelper;
import org.apache.lucene.index.*;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PositionMatchScorer extends Scorer {
    private final int NOT_FOUND_POSITION = -1;
    private final int HALF_SCORE_POSITION = 100; // position where score should decrease by 50%

    private final LeafReaderContext context;
    private final Scorer scorer;

    PositionMatchScorer(PositionMatchWeight weight, LeafReaderContext context) throws IOException {
        super(weight);
        this.context = context;
        this.scorer = weight.weight.scorer(context);
    }

    @Override
    public DocIdSetIterator iterator() {
        return scorer.iterator();
    }

    @Override
    public float getMaxScore(int upTo) throws IOException {
        return scorer.getMaxScore(upTo);
    }

	@Override
    public float score() {
        int doc = docID();
        float totalScore = 0.0f;

        Set<Term> terms = new HashSet<>();
        weight.extractTerms(terms);
        
        for (Term term : terms) {
            totalScore += scoreTerm(doc, term);
        }

        return totalScore;
    }

    Explanation explain(int docID) throws IOException {
        List<Explanation> explanations = new ArrayList<>();

        float totalScore = 0.0f;

        Set<Term> terms = new HashSet<>();
        weight.extractTerms(terms);

        for (Term term : terms) {
            Explanation termExplanation = explainTerm(docID, term);
            explanations.add(termExplanation);
            totalScore += termExplanation.getValue().floatValue();
        }

        return Explanation.match(
            totalScore,
            String.format("score(doc=%d), sum of:", docID),
            explanations
        );
    }

    @Override
    public int docID() {
        return scorer.docID();
    }

    private float scoreTerm(int docID, Term term) {
        float termScore = 0.0f; // default score
        float termPosition = Math.abs(position(docID, term));
        float tfValue = tf(docID, term,"TFIDF");
        float idfValue = idf(docID, term,"TFIDF");
        if (NOT_FOUND_POSITION != termPosition) 
        	termScore = tfValue*idfValue;
        	//termScore = bm25(docID, term);
        return termScore;
    }

    private Explanation explainTerm(int docID, Term term) throws IOException {
        float termPosition = Math.abs(position(docID, term));
        Terms terms = context.reader().getTermVector(docID, term.field());
        float tfValue = tf(docID, term,"TFIDF");
        float idfValue = idf(docID, term,"TFIDF");
        int n = context.reader().docFreq(term);
        int N = context.reader().getDocCount(term.field());
        
        long dl = terms.size();
        
        if (NOT_FOUND_POSITION == termPosition) {
            return Explanation.noMatch(
                String.format("no matching terms for field=%s, term=%s", term.field(), term.text())
            );
        } else {
            float termScore = tfValue*idfValue;
            String func =  "payload = "+termPosition+" tf = "+tfValue+" idf = "+idfValue+" n value = " + n + " N value is =" + N + " dl is =" + dl;
            //String func =  "payload = "+termPosition+" bm25 score value = "+termScore +" n value = " + n + " N value is =" + N + " dl is =" + dl;
            return Explanation.match(
                termScore,
                String.format("score(field=%s, term=%s, score=%f, func=%s)", term.field(), term.text(), termScore, func)
            );
        }
    }

    private float position(int docID, Term term) {
        try {
            Terms terms = context.reader().getTermVector(docID, term.field());
            if (terms == null) {
                return NOT_FOUND_POSITION;
            }
            TermsEnum termsEnum = terms.iterator();
            if (!termsEnum.seekExact(term.bytes())) {
                return NOT_FOUND_POSITION;
            }
            PostingsEnum dpEnum = termsEnum.postings(null, PostingsEnum.ALL);
            dpEnum.nextDoc();
            dpEnum.nextPosition();
            BytesRef payload = dpEnum.getPayload();
            /*int docCount = context.reader().getDocCount(term.field());
            int docFreq = context.reader().docFreq(term);
            double idf = Math.log(docCount/docFreq);
             */
            if (payload == null) {
                return NOT_FOUND_POSITION;
            }
            //return (float) (PayloadHelper.decodeFloat(payload.bytes, payload.offset)*dpEnum.freq()*idf);
            return PayloadHelper.decodeFloat(payload.bytes, payload.offset);
        } catch (UnsupportedOperationException ex) {
            LogManager.getLogger(this.getClass()).error("Unsupported operation, returning position = " +
                NOT_FOUND_POSITION + " for field = " + term.field());
            return NOT_FOUND_POSITION;
        } catch (Exception ex) {
            LogManager.getLogger(this.getClass()).error("Unexpected exception, returning position = " +
                NOT_FOUND_POSITION + " for field = " + term.field());
            return NOT_FOUND_POSITION;
        }
    }
    
    private float tf(int docID, Term term, String type) {
        try {
            Terms terms = context.reader().getTermVector(docID, term.field());
            if (terms == null) {
                return NOT_FOUND_POSITION;
            }
            TermsEnum termsEnum = terms.iterator();
            if (!termsEnum.seekExact(term.bytes())) {
                return NOT_FOUND_POSITION;
            }
            PostingsEnum dpEnum = termsEnum.postings(null, PostingsEnum.ALL);
            dpEnum.nextDoc();
            dpEnum.nextPosition();
            int freq = dpEnum.freq();
            double tf;
            if(type.equalsIgnoreCase("BM25")) {
            	double k1 = 1.2;
                double b = 0.75;
                
                long dl = terms.size();
                //double avgdl = 2725.3328; //webap
                double avgdl = 673.4601 ; //trec news
                //double avgdl = 664.1846; //news 2019
                //double avgdl = 95.81751; //citeulike
                
                //tf, computed as freq / (freq + k1 * (1 - b + b * dl / avgdl))
                tf = freq / (freq + k1 * (1 - b + b * dl / avgdl));
            }else {
            	tf = freq;
            }
            
            return (float) (tf);
            
        } catch (UnsupportedOperationException ex) {
            LogManager.getLogger(this.getClass()).error("Unsupported operation, returning position = " +
                NOT_FOUND_POSITION + " for field = " + term.field());
            return NOT_FOUND_POSITION;
        } catch (Exception ex) {
            LogManager.getLogger(this.getClass()).error("Unexpected exception, returning position = " +
                NOT_FOUND_POSITION + " for field = " + term.field());
            return NOT_FOUND_POSITION;
        }
    }
    
    private float idf(int docID, Term term,String type) {
        try {
            Terms terms = context.reader().getTermVector(docID, term.field());
            if (terms == null) {
                return NOT_FOUND_POSITION;
            }
            TermsEnum termsEnum = terms.iterator();
            if (!termsEnum.seekExact(term.bytes())) {
                return NOT_FOUND_POSITION;
            }
            PostingsEnum dpEnum = termsEnum.postings(null, PostingsEnum.ALL);
            dpEnum.nextDoc();
            dpEnum.nextPosition();
            int docCount = context.reader().getDocCount(term.field());
            int docFreq = context.reader().docFreq(term);
            double idf;
            if(type.equalsIgnoreCase("BM25")) {
            	//idf, computed as log(1 + (N - n + 0.5) / (n + 0.5))
            	//int N = 20909;
                idf = Math.log(1+(docCount-docFreq+0.5)/(docFreq+0.5));
                //idf = Math.log(1+(docCount-docFreq)/(docFreq));
            }else {
            	idf = Math.log(1+(docCount/docFreq));
            }
            
            return (float) (idf);
            
        } catch (UnsupportedOperationException ex) {
            LogManager.getLogger(this.getClass()).error("Unsupported operation, returning position = " +
                NOT_FOUND_POSITION + " for field = " + term.field());
            return NOT_FOUND_POSITION;
        } catch (Exception ex) {
            LogManager.getLogger(this.getClass()).error("Unexpected exception, returning position = " +
                NOT_FOUND_POSITION + " for field = " + term.field());
            return NOT_FOUND_POSITION;
        }
    }
    
}

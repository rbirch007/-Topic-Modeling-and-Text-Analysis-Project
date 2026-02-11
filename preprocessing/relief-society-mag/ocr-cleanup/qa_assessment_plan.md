# OCR Cleanup Quality Assessment Plan

## Sample Size: 36 files (stratified random sample across decades)

## Error Categories to Assess

### 1. **Character Substitution Errors**
- Single character OCR misreads (©→C, ®→R, ¥→Y, etc.)
- Context-dependent substitutions (© could be C, O, or ©)
- Status: Should be caught by regex

### 2. **Word-Level Errors**
- Common patterns: "tlie"→"the", "witli"→"with", "cliurch"→"church"
- Less common patterns we may have missed
- Status: Partially caught by regex

### 3. **Proper Noun Errors**
- Names: "Josel»h Smith", "Brigham ¥oung"
- Places: Geographic names with OCR errors
- Status: Likely NOT caught by regex

### 4. **Complex Multi-Character Garbles**
- Sequences of corrupted characters
- Context-dependent corrections needed
- Status: NOT caught by regex

### 5. **Punctuation & Structural Errors**
- Sentence boundaries
- Quote marks (should be caught)
- Paragraph breaks
- Status: Partially caught

### 6. **Context-Dependent Ambiguities**
- Words that could be multiple things
- Numbers vs. letters
- Status: NOT caught by regex

## LLM Parameter Optimization

### Model Selection:
- **Haiku** ($0.25/$1.25 per M tokens): Fast, cheap, good for evaluation
- **Sonnet** ($3/$15 per M tokens): Better quality, use only if Haiku insufficient
- **Recommendation**: Start with Haiku

### Processing Strategy:
1. **Sample extraction**: Take 3-5 random paragraphs (~500-1000 words) from each file
2. **Comparison**: Show raw vs cleaned side-by-side
3. **Evaluation task**: Count errors by category, not full correction
4. **Output**: Structured JSON for easy aggregation

### Cost Estimation (Haiku):
- 36 files × ~1000 words/sample × 1.5 tokens/word = ~54K tokens input
- Raw text comparison: ~54K tokens additional input = 108K total input
- Evaluation output: ~5K tokens (structured)
- **Total cost**: ~$0.03 input + ~$0.01 output = **~$0.04 total**

### Speed Optimization:
- Process in parallel (multiple files at once)
- Use structured output format
- Keep prompts concise
- Focus on counting, not fixing

## Evaluation Metrics

For each sample:
1. **Total OCR errors remaining** (count)
2. **Errors by category** (breakdown)
3. **Severity**: Critical (changes meaning) vs. Minor (typos)
4. **False positives**: Incorrect "fixes" by regex
5. **Overall quality score**: 0-100

## Deliverable

Summary report with:
- Error rate per file
- Error rate by decade
- Most common missed error types
- Recommendation: Is additional LLM cleanup needed?
- Estimated cost if full LLM cleanup desired

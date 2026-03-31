package anomaly

// IsolationForest implements the Isolation Forest anomaly detection algorithm.
//
// Reference: Liu, F.T., Ting, K.M., Zhou, Z.H. (2008). Isolation Forest.
// IEEE International Conference on Data Mining, pp. 413-422.
//
// 알고리즘 요약:
//   - n_trees 개의 iTree를 각각 maxSamples개 랜덤 부분집합으로 독립 학습
//   - 각 iTree는 랜덤 feature + 랜덤 split value로 데이터를 분기해 트리를 구성
//   - 이상치는 정상 데이터보다 더 얕은 깊이에서 고립된다 → 경로 길이가 짧다
//   - Score(x) = 2^(-avg_path / c(n)): 0.5 = 정상, 1.0 = 완전 이상
//
// Go 구현 특이사항:
//   - 외부 의존성 없이 math/rand만 사용
//   - 학습과 추론이 분리 (Fit / Score)되어 재학습 중에도 이전 모델로 추론 가능

import (
	"math"
	"math/rand"
	"time"
)

// iNode는 iTree의 내부 노드 또는 리프 노드.
type iNode struct {
	// 내부 노드
	feature  int
	splitVal float64
	left     *iNode
	right    *iNode
	// 리프 노드
	isLeaf bool
	size   int // 이 노드에 도달한 샘플 수 (미완 분기 보정용)
}

// IsolationForest는 다변량 이상 탐지기.
type IsolationForest struct {
	nTrees     int
	maxSamples int
	trees      []*iNode
	// seed != 0 이면 Fit() 호출마다 동일한 RNG를 사용한다 (재현 가능 모드).
	// seed == 0 이면 time.Now().UnixNano()로 매번 새로운 RNG를 생성한다 (기본).
	seed int64
}

// NewIsolationForest는 IsolationForest를 생성한다.
// nTrees ≤ 0이면 100, maxSamples ≤ 0이면 256을 사용한다.
// 시드는 0(랜덤)으로 초기화된다.
func NewIsolationForest(nTrees, maxSamples int) *IsolationForest {
	if nTrees <= 0 {
		nTrees = 100
	}
	if maxSamples <= 0 {
		maxSamples = 256
	}
	return &IsolationForest{nTrees: nTrees, maxSamples: maxSamples}
}

// NewIsolationForestWithSeed는 고정 시드로 IsolationForest를 생성한다.
// 테스트 또는 재현 가능한 실험에서 사용한다.
// seed == 0 이면 NewIsolationForest와 동일하게 랜덤 시드를 사용한다.
func NewIsolationForestWithSeed(nTrees, maxSamples int, seed int64) *IsolationForest {
	f := NewIsolationForest(nTrees, maxSamples)
	f.seed = seed
	return f
}

// Fit trains the forest on data (n_samples × n_features).
// data는 이미 동일 스케일로 정규화된 상태여야 한다 (호출자 책임).
// f.seed != 0 이면 고정 시드로 재현 가능한 트리를 구성한다.
func (f *IsolationForest) Fit(data [][]float64) {
	if len(data) == 0 {
		return
	}
	var src int64
	if f.seed != 0 {
		src = f.seed
	} else {
		src = time.Now().UnixNano()
	}
	rng := rand.New(rand.NewSource(src))
	maxDepth := int(math.Ceil(math.Log2(float64(f.maxSamples))))
	f.trees = make([]*iNode, f.nTrees)
	for i := range f.trees {
		sample := iSubsample(data, f.maxSamples, rng)
		f.trees[i] = buildITree(sample, 0, maxDepth, rng)
	}
}

// Score returns the anomaly score for x in [0, 1].
// 0.5 = 정상 범위, > 0.65 = 이상 의심, > 0.75 = 강한 이상.
// trees가 없으면(학습 전) 0을 반환한다.
func (f *IsolationForest) Score(x []float64) float64 {
	if len(f.trees) == 0 {
		return 0
	}
	var total float64
	for _, t := range f.trees {
		total += pathLen(x, t, 0)
	}
	avg := total / float64(len(f.trees))
	return math.Pow(2, -avg/cFactor(f.maxSamples))
}

// Trained reports whether the forest has been fitted.
func (f *IsolationForest) Trained() bool {
	return len(f.trees) > 0
}

// --- 내부 함수 ---

func iSubsample(data [][]float64, n int, rng *rand.Rand) [][]float64 {
	if len(data) <= n {
		return data
	}
	perm := rng.Perm(len(data))
	out := make([][]float64, n)
	for i := 0; i < n; i++ {
		out[i] = data[perm[i]]
	}
	return out
}

func buildITree(data [][]float64, depth, maxDepth int, rng *rand.Rand) *iNode {
	n := len(data)
	if n <= 1 || depth >= maxDepth {
		return &iNode{isLeaf: true, size: n}
	}

	nFeatures := len(data[0])
	feat := rng.Intn(nFeatures)

	minV, maxV := data[0][feat], data[0][feat]
	for _, row := range data[1:] {
		if row[feat] < minV {
			minV = row[feat]
		}
		if row[feat] > maxV {
			maxV = row[feat]
		}
	}
	if minV == maxV {
		return &iNode{isLeaf: true, size: n}
	}

	split := minV + rng.Float64()*(maxV-minV)

	var left, right [][]float64
	for _, row := range data {
		if row[feat] < split {
			left = append(left, row)
		} else {
			right = append(right, row)
		}
	}

	return &iNode{
		feature:  feat,
		splitVal: split,
		left:     buildITree(left, depth+1, maxDepth, rng),
		right:    buildITree(right, depth+1, maxDepth, rng),
	}
}

// pathLen은 point가 트리에서 이동한 경로 길이를 계산한다.
// 리프 도달 시 미완 분기를 cFactor(size)로 보정한다.
func pathLen(x []float64, node *iNode, depth float64) float64 {
	if node.isLeaf {
		return depth + cFactor(node.size)
	}
	if x[node.feature] < node.splitVal {
		return pathLen(x, node.left, depth+1)
	}
	return pathLen(x, node.right, depth+1)
}

// cFactor는 n개 샘플이 있는 이진 탐색 트리의 평균 경로 길이.
// c(n) = 2*H(n-1) - 2*(n-1)/n, H = 조화급수 (오일러-마스케로니 근사).
func cFactor(n int) float64 {
	if n <= 1 {
		return 0
	}
	// H(n-1) ≈ ln(n-1) + γ
	h := math.Log(float64(n-1)) + 0.5772156649015329
	return 2*h - 2*float64(n-1)/float64(n)
}

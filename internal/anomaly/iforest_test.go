package anomaly

import (
	"math/rand"
	"testing"
)

// TestIsolationForest_Deterministic는 동일한 시드로 Fit()을 두 번 호출하면
// 동일한 Score()를 반환함을 검증한다.
func TestIsolationForest_Deterministic(t *testing.T) {
	data := makeTestData(200, 5, rand.New(rand.NewSource(42)))

	f1 := NewIsolationForestWithSeed(50, 64, 42)
	f1.Fit(data)

	f2 := NewIsolationForestWithSeed(50, 64, 42)
	f2.Fit(data)

	probe := []float64{0.1, 0.1, 0.1, 0.01, 0.9}
	s1 := f1.Score(probe)
	s2 := f2.Score(probe)
	if s1 != s2 {
		t.Errorf("동일 시드 두 인스턴스 점수 불일치: s1=%.6f s2=%.6f", s1, s2)
	}
}

// TestIsolationForest_AnomalyHigherScore는 이상치가 정상 데이터보다 높은 점수를 받음을 확인한다.
func TestIsolationForest_AnomalyHigherScore(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	// 정상 데이터: 모든 feature ≈ [0.1, 0.2] 범위
	data := makeTestData(300, 5, rng)

	f := NewIsolationForestWithSeed(100, 128, 42)
	f.Fit(data)

	normal := []float64{0.15, 0.15, 0.15, 0.02, 0.5}
	outlier := []float64{5.0, 5.0, 5.0, 1.0, 0.0} // 극단적 이상치

	normalScore := f.Score(normal)
	outlierScore := f.Score(outlier)

	if outlierScore <= normalScore {
		t.Errorf("이상치 점수(%.4f)가 정상 점수(%.4f) 이하여서는 안 됨", outlierScore, normalScore)
	}
}

// TestIsolationForest_BeforeFit은 학습 전 Score()가 0을 반환함을 확인한다.
func TestIsolationForest_BeforeFit(t *testing.T) {
	f := NewIsolationForest(10, 32)
	if f.Trained() {
		t.Error("Fit() 전에 Trained()가 true여서는 안 됨")
	}
	if s := f.Score([]float64{1.0, 2.0}); s != 0 {
		t.Errorf("Fit() 전 Score() = %.4f, want 0", s)
	}
}

// TestIsolationForest_EmptyData는 빈 데이터로 Fit()을 호출해도 패닉이 없음을 확인한다.
func TestIsolationForest_EmptyData(t *testing.T) {
	f := NewIsolationForestWithSeed(10, 32, 42)
	f.Fit(nil)
	if f.Trained() {
		t.Error("빈 데이터로 Fit() 후 Trained()가 true여서는 안 됨")
	}
}

// TestIsolationForest_ScoreRange는 Score()가 [0, 1] 범위 내에 있음을 확인한다.
func TestIsolationForest_ScoreRange(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	data := makeTestData(200, 5, rng)

	f := NewIsolationForestWithSeed(50, 64, 42)
	f.Fit(data)

	probes := [][]float64{
		{0.1, 0.1, 0.1, 0.01, 0.5},
		{1.0, 1.0, 1.0, 1.0, 1.0},
		{0.0, 0.0, 0.0, 0.0, 0.0},
		{10.0, 10.0, 10.0, 10.0, 10.0},
	}
	for _, p := range probes {
		s := f.Score(p)
		if s < 0 || s > 1 {
			t.Errorf("Score(%v) = %.4f, 범위 [0,1] 초과", p, s)
		}
	}
}

// makeTestData는 [0, 1] 범위의 정규분포 근사 데이터를 생성한다.
func makeTestData(n, features int, rng *rand.Rand) [][]float64 {
	data := make([][]float64, n)
	for i := range data {
		row := make([]float64, features)
		for j := range row {
			v := rng.Float64()*0.2 + 0.1 // [0.1, 0.3] 범위 정상 데이터
			row[j] = v
		}
		data[i] = row
	}
	return data
}

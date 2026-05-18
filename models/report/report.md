## Modeling và thực nghiệm
### Cơ sở và mục tiêu
- Nhằm đưa ra lời khẳng định về tính khả thi của framework trên cơ sở minh chứng thực nghiệm định lượng khách quan: Chứng minh sự cân bằng tối ưu giữa *hiệu năng phân lớp (classification performance) tổng thể* và *độ nhạy của mô hình đối với các nhóm dữ liệu thiểu số (vốn chiếm tỷ trọng nhỏ nhưng mang sắc thái cảm xúc cao)*.
- Chứng minh được tính thực tiễn và độ tương thích của framework với các kiến trúc Transformer hiện đại: Quy trình thực nghiệm sẽ trải qua quá trình tiến hành tinh chỉnh (fine-tuning) dữ liệu thu được từ giai đoạn trước đó với các mô hình mang tính đại diện, nhằm chứng minh khả năng thích ứng linh hoạt và tối ưu của chúng đối với tập dữ liệu bình luận thực tế đã được thu thập và tiền xử lý trong các giai đoạn trước.

### Dữ liệu huấn luyện
- Dữ liệu gồm có hơn 3 triệu dòng. Nhưng có phân phối lệch về nhãn neutral và các nhãn mang sắc thái tích cực như Figure 1. Mà nguyên nhân đó cũng phù hợp với thiên hướng cảm xúc mong muốn tích cực của con người.

![Phân phối dữ liệu đầu vào theo các nhãn](./data/label_distribution.png)

- Do đó cần phải thực hiện giai đoạn unsampling để cân bằng dữ liệu. Mà trong ngữ cảnh này chính là unsampling dữ liệu có nhãn neutral. Và kết quả thu được là dữ liệu có phân phối trong Figure 2.

![Phân phối dữ liệu sau khi unsampling](./data/label_dist_after_unsampling.png)

### Xử lý imbalancing
- Tuy dù đãunsampling thì dữ liệu thực tế cũng đã mất cân bằng giữa các cảm xúc tích cực và tiêu cực. Nhất là các cảm xúc vi mô hiếm như (sợ hãi - `fear`, lo lắng - `nervousness`, thương tiếc - `grief`,...) thì càng ít hơn.
- Do đó, nếu huấn luyện hay tinh chỉnh trên dữ liệu này một cách dập khuôn thì hiệu suất của mô hình sẽ rất thấp, và thậm chí mô hình sẽ cực đoan mà tổng quát hóa trên dữ liệu tích cực và át đi các nhãn hiếm tiêu cực.
- Cho nên, trong phạm vi khóa luận, tôi đề xuất áp dụng kết hợp Class-Balanced Loss (CB-Loss) [1] và Gradient Harmonized Mechanism Loss (GHM Loss) [2] để hiệu chỉnh (regularized) quá trình học của mô hình. Cách làm này kết hợp khả năng định hướng nhờ sự đóng góp của từng lớp vào giá trị loss của CB Loss và khả năng điều tiết trọng số dựa trên độ khó thực tế của từng mẫu dữ liệu của GHM Loss.
- Sự cộng hưởng này giúp mô hình vừa không bị áp đảo bởi các lớp có số lượng mẫu lớn, vừa tránh được hiện tượng overfit vào các mẫu nhiễu (outliers). Mà cụ thể, ta có thể biểu diễn cách làm bằng công thức toán học như sau:

Giả sử một mẫu dữ liệu có nhãn thực (`true label`) là $y \in \{1, 2, ..., C\}$ (với $C$ là số lượng lớp - `label`) và xác suất dự đoán của mô hình cho lớp $y$ là $p_{y}$. Gọi $g$ là độ lớn gradient của mẫu dữ liệu đối với đầu ra của mô hình, được định nghĩa bằng $g = |p_{y} - 1|$.

**Class-Balanced Loss** được định nghĩa nhằm để giải quyết bài toán mất cân bằng dữ liệu bằng cách đưa số lượng mẫu hiệu dụng (effective number of samples) để tính trọng số cân bằng của một lớp cụ thể $y$ với $$W_{y} = \frac{1 - \beta}{1 - \beta^{n_{y}}}$$

 Trong đó:
 - $n_{y}$ là số lượng mẫu thô của lớp $y$ trong toàn bộ tập dữ liệu huấn luyện.
 - $\beta \in [0, 1)$ là một siêu tham số điều chỉnh (thông thường $\beta = 0.99, 0.999$).

Khi đó, hàm mất mát CB-Loss áp dụng trên hàm Cross-Entropy (CE) tiêu chuẩn có dạng:
$$\mathcal{L}_{\text{CB}}(p_y) = -\frac{1 - \beta}{1 - \beta^{n_y}} \log(p_y)$

**Gradient Harmonized Mechanism Loss (GHM-Loss)** được đưa ra nhằm điều chỉnh trọng số của mẫu dựa trên *Mật độ Gradient (Gradient Density)* để giảm bớt ảnh hưởng của các mẫu quá dễ hoặc các mẫu nhiễu cực khó. Và hàm mật độ gradient của một giá trị $g$ được xác định bằng:
$$GD(g) = \frac{1}{R_{i}} \sum_{k=1}^{N} \delta(g_{k}, g)$$

Trong đó:
- $N$ là tổng số mẫu trong một batch huấn luyện (`training_batch`).
- $g_{k}$ là độ lớn gradient của mẫu thứ $k$.
- $\delta(g_{k}, g)$ là hàm chỉ dấu, bằng $1$ nếu $g_{k}$ nằm trong khoảng (bin) của $g$ và bằng $0$ nếu ngược lại.
- $R_{i}$ là độ rộng của khoảng chứa $g$.

Với *hệ số điều hòa* GHM được tính bằng nghịch đảo của mật độ $\hat{\beta}_{i}=\frac{N}{GD(g)}$. Thì hàm mất mát GHM-Loss được định nghĩa là:
$$\mathcal{L}_{\text{GHM}} = \frac{1}{N} \sum_{i=1}^{N} \frac{-\log(p_{y, i})}{GD(g_{i})}$$

Và để đồng thời tối ưu hóa cả hai khía cạnh, **cân bằng số lượng mẫu hiệu dụng giữa các lớp** và **điều hòa độ khó của từng mẫu riêng lẻ**, hàm **Mixure Loss** được triển khai như sau:
$$\mathcal{L}_{\text{Mixture}} = W_y \cdot \mathcal{L}_{\text{GHM}}$$

### Các mô hình

Vì framework chỉ giới hạn trong khuôn khổ dựa trên các kiến trúc trannsformer encoder. Do đó, chúng ta chỉ giới hạn trong các mô hình Encoder dòng họ Bert. Và theo khung thời gian ta chỉ chọn ba đại diện. Lần lượt.
- **mBERT** [3], được phát hành bởi Google cùng thời điểm với BERT, và mBERT cũng là một trong những tượng đài đầu tiên của kỷ nguyên tiền huấn luyện đa ngôn ngữ với quá trình huấn luyện (`pre-trained`) đồng thời trên dữ liệu Wikipedia trải dài trên 104 ngôn ngữ khác nhau (bao gồm cả tiếng Việt).
- **phoBERT** [4], là mô hình Monolingual (Đơn ngôn ngữ), được huấn luyện hoàn toàn bằng một kho dữ liệu tiếng Việt khổng lồ (30GB văn bản thô từ báo chí, văn học, mạng xã hội) dựa trên kiến trúc RoBERTa. Đồng thời dữ liệu đầu vào cũng được phân tiết (ví dụ `học sinh` => `học_sinh`) nhờ đó mà mô hình hiểu được trực tiếp các khái niệm ngữ nghĩa ở cấp độ từ trong Tiếng Việt.
- **mModernBERT** [5], là phiên bản `Multilingual` của mô hình ModernBERT. Đây cũng là kiến trúc được hiệu chỉnh rất sâu so với kiến trúc của mô hình BERT ban đầu (vốn đã tồn tại hơn 6 năm). Nó đã được mở rộng độ dài ngữ cảnh lên tới 8192 tokens, tích hợp cơ chế tính toán Attention siêu nhanh. Thay thế cơ chế mã hóa vị trí tuyệt đối cũ bằng RoPE (giống như Llama) giúp mô hình nắm bắt vị trí và khoảng cách của từ trong câu tốt hơn, tăng khả năng nhạy bén với cấu trúc ngữ pháp lỏng lẻo của ngôn ngữ mạng xã hội.

### Các độ đo
- **F1-Macro**, là trung bình cộng của các chỉ số F1-Score được tính riêng biệt cho từng lớp (class), không phụ thuộc vào số lượng mẫu của lớp đó nhiều hay ít.
$$F1_{C} = 2 \times \frac{Precision_{C} \times Recall_{C}}{Precision_{C} + Recall_{C}}$$
$$F1_{macro} = \frac{F1_{Class_{1}} + F1_{Class_{2}} + ... + F1_{Class_{N}}}{N}$$
- **Accuracy Balanced**, được thiết kế đặc biệt để đánh giá mô hình trên tập dữ liệu mất cân bằng. Nó tính toán dựa trên độ chính xác (Recall) của từng lớp riêng biệt.
$$Balanced\ Accuracy = \frac{\sum_{i=1}^{N} Recall_{Class_{i}}}{N}$$
- **Average Accuracy** hay **Overall Accuracy**, là độ chính xác tổng thể truyền thống của toàn bộ mô hình trên tập dữ liệu kiểm định.
$$\text{Accuracy} = \frac{\text{Số lượng mẫu đoán đúng}}{\text{Tổng số mẫu trong tập dữ liệu}}$$

### Kết quả
![Quá trình hiệu chỉnh mô hình mBERT](./report/mbert/epoch_metrics_chart.png){fig-pos="H"}

![Quá trình hiệu chỉnh mô hình phoBERT](./report/phobert/epoch_metrics_chart.png){fig-pos="H"}

![Quá trình hiệu chỉnh mô hình mModernBERT](./report/mmodernbert/epoch_metrics_chart.png){fig-pos="H"}

Quá trình huấn luyện trên tập dữ liệu lớn với gần 3 triệu điểm dữ liệu và với 28 lớp không cân bằng (`imbalance`) cho thấy hiện tượng phân kỳ sớm của Validation Loss. Tuy nhiên, nếu ta đi vào phân tích sâu hơn, thì có thể nhận thấy rằng đây hoàn toàn là đặc trưng toán học mong muốn khi kết hợp giữa CB-Loss và GHM Loss nhằm bảo vệ các lớp thiểu số.

Đồng thời, hiệu năng thực tế của mô hình (thể hiện qua F1-Macro và Balanced Accuracy) vẫn không hề suy giảm mà vẫn tăng trưởng ổn định. Đặc biệt, nghiên cứu cũng xác định rằng điểm dừng lý tưởng (Early Stopping) rơi vào khoảng 3-5 epoch đầu tiên.

Và với mModernBERT là mô hình mang lại hiệu năng đột phá (F1-Macro ~0.89) chỉ trong sáu vòng huấn luyện (epoch) so với hai mô hình mBERT và phoBERT, điều này chứng tỏ mModerBERT đạt được sự tối ưu cả về độ chính xác lẫn chi phí huấn luyện.

Dưới đây là bảng tổng hợp kết quả kiểm định. 

| model | F1 macro | Overall acc | Balanced acc |
| :--- | :---: | ---: | :---: |
| mBERT | 82.19 ± 0.38% | 82.93 ± 0.19% | 82.35 ± 0.44% |
| phoBERT | 82.99 ± 0.36% | 83.84 ± 0.18% | 82.99 ± 0.43% |
| mModernBERT | 88.94 ± 0.34% | 91.53 ± 0.14% | 89.80 ± 0.38% |

---

### Trích dẫn

[1] Cui, Y., Jia, M., Lin, T. Y., Song, Y., & Belongie, S. (2019). Class-balanced loss based on effective number of samples. In Proceedings of the IEEE/CVF Conference on Computer Vision and Pattern Recognition (pp. 9268–9277). https://doi.org/10.1109/CVPR.2019.00949

[2] Li, B., Liu, Y., & Wang, X. (2019). Gradient harmonized single-stage detector. In Proceedings of the AAAI Conference on Artificial Intelligence (Vol. 33, No. 01, pp. 8577–8584). https://doi.org/10.1609/aaai.v33i01.33018577

[3] Devlin, J., Chang, M. W., Lee, K., & Toutanova, K. (2018). BERT: Pre-training of deep bidirectional transformers for language understanding. arXiv preprint arXiv:1810.04805. https://doi.org/10.48550/arXiv.1810.04805

[4] Nguyen, D. Q., & Tuan, A. T. (2020). PhoBERT: Pre-trained language models for Vietnamese. In Findings of the Association for Computational Linguistics: EMNLP 2020 (pp. 1037–1042). https://doi.org/10.18653/v1/2020.findings-emnlp.92

[5] Rubin, J., Young, B., Sanseviero, O., Schmid, J., & Rush, A. M. (2024). ModernBERT: A modernized transformer encoder for the hardware era. arXiv preprint arXiv:2412.13663. https://doi.org/10.48550/arXiv.2412.13663

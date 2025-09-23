
## 📦 Data Pipeline – MNIST Image Classification using PyTorch

A beginner-friendly deep learning project that implements an image classification pipeline using PyTorch. The model is trained to classify handwritten digits from the MNIST dataset and visualizes training and testing accuracy.

---

### 🧠 Project Type

**Deep Learning Project – Image Classification**

---

### ✨ Features

* Built using **PyTorch** (works with Python 3.13+)
* Trains a **neural network** on the MNIST dataset
* Visualizes **accuracy over epochs**
* Fully compatible with **CPU or GPU**
* Clean and modular code, ideal for beginners

---

### 🧰 Tech Stack

| Tool         | Description             |
| ------------ | ----------------------- |
| Python 3.10+ | Programming language    |
| PyTorch      | Deep Learning framework |
| Torchvision  | MNIST dataset           |
| Matplotlib   | Data visualization      |

---

### 📁 Project Structure

```
mnist_pytorch/
│
├── mnist_pytorch.py       # Main training script
├── data/                  # Automatically created for MNIST data
├── README.md              # This file
└── requirements.txt       # Python dependencies (optional)
```

---

### ⚙️ Installation

#### Step 1: Clone the repo

```bash
git clone https://github.com/yourusername/mnist-pytorch-pipeline.git
cd mnist-pytorch-pipeline
```

#### Step 2: Install packages

```bash
pip install torch torchvision matplotlib
```

---

### ▶️ Run the Model

```bash
python mnist_pytorch.py
```

This will:

* Download the MNIST dataset
* Train the model for 5 epochs
* Display training & test accuracy
* Plot accuracy over epochs

---

### 📊 Sample Output

> Test Accuracy after 5 epochs: \~97%
> *(Insert plot screenshot here if needed)*

---

### 📌 Future Improvements

* Add model saving/loading (`.pt`)
* Use custom datasets
* Implement convolutional layers for better performance
* Build a simple GUI using Tkinter or Gradio

---

### 👨‍💼 Author

**Your Name**
📧 [your.email@example.com](mailto:your.email@example.com)
🔗 [GitHub](https://github.com/yourusername)

---

### 📄 License

This project is licensed under the [MIT License](LICENSE).

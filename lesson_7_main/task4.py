import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy import stats


def load_data(filepath):
    data = pd.read_csv(filepath)
    return data


def analyze_correlation(data):
    correlation = data[["GoalsScored", "GoalDiff"]].corr()["GoalsScored"]["GoalDiff"]
    return round(correlation, 2)


def create_scatter_plot(data, correlation):
    goals_scored = data["GoalsScored"]
    goal_diff = data["GoalDiff"]
    positions = data["Position"]

    # Calculating the regression line
    slope, intercept, _, _, _ = stats.linregress(goals_scored, goal_diff)

    plt.figure(figsize=(12, 8))

    scatter = plt.scatter(
        goals_scored,
        goal_diff,
        c=positions,
        cmap="RdYlGn_r",
        s=80,
        alpha=0.6,
        edgecolors="black",
        linewidth=0.5,
    )

    # draw regression line
    x_line = np.linspace(goals_scored.min(), goals_scored.max(), 100)
    y_line = slope * x_line + intercept
    plt.plot(
        x_line,
        y_line,
        "r--",
        linewidth=3,
        label=f"Регрессия: y = {slope:.3f}x {intercept:.3f}\nR = {correlation}",
        zorder=10,
    )

    # 0 line
    plt.axhline(
        y=0,
        color="gray",
        linestyle=":",
        linewidth=2,
        alpha=0.7,
        label="Нулевая разность",
    )

    # Colorbar для позиций
    cbar = plt.colorbar(scatter)
    cbar.set_label("Позиция в таблице", fontsize=12, fontweight="bold")

    plt.xlabel("Забитые голы", fontsize=13, fontweight="bold")
    plt.ylabel("Разность голов", fontsize=13, fontweight="bold")
    plt.title(
        "Зависимость разности голов от забитых голов",
        fontsize=14,
        fontweight="bold",
    )
    plt.grid(True, alpha=0.3, linestyle="--")
    plt.legend(loc="upper left", fontsize=11)

    # Добавляем текстовую метку под легендой
    plt.text(
        0.01,
        0.85,
        f"Вывод:\nТ.к. R = {correlation} , то наблюдается сильная положительная\nлинейная зависимость между\nзабитыми голами и разностью голов.",
        transform=plt.gca().transAxes,
        fontsize=10,
        verticalalignment="top",
        bbox=dict(boxstyle="round", facecolor="wheat", alpha=0.5),
    )

    plt.tight_layout()


def main():
    data = load_data("./eplleaguetables.csv")

    correlation = analyze_correlation(data)

    # Создание визуализации
    create_scatter_plot(data, correlation)

    plt.show()


if __name__ == "__main__":
    main()

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


def load_data(filepath):
    data = pd.read_csv(filepath)
    return data


def analyze_relegation_zone(data):
    # Команды на 17 месте - первое РЕАЛЬНОЕ безопасное место , но по условию задачи и 18 место безопасно, но это не оч логично

    # Максимальное количество очков среди вылетевших команд
    max_relegated_points = data[data["Position"].isin([19, 20])]["Points"].max()

    # Минимальное количество очков среди команд на 17 месте
    min_safe_points = data[data["Position"] == 17]["Points"].min()

    return max_relegated_points, min_safe_points


def create_relegation_visualization(
    data: pd.DataFrame, max_relegated_points, min_safe_points
):

    positions = [17, 18, 19, 20]
    data_by_position = {}

    for pos in positions:
        data_by_position[pos] = data[data["Position"] == pos]["Points"].values

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 7))

    # Боксплот по позициям 17-20 
    bp = ax1.boxplot(
        [data_by_position[pos] for pos in positions],
        positions=positions,
        widths=0.5,
        patch_artist=True,
        showfliers=True,
        flierprops={
            "marker": "o",
            "markerfacecolor": "red",
            "markersize": 8,
            "linestyle": "none",
            "markeredgecolor": "darkred",
            "alpha": 0.7,
        },
        boxprops={"facecolor": "lightcoral", "edgecolor": "black", "linewidth": 2},
        medianprops={"color": "darkred", "linewidth": 2.5},
        whiskerprops={"color": "black", "linewidth": 1.5},
        capprops={"color": "black", "linewidth": 1.5},
    )

    # Закрашиваем безопасную зону зеленым
    bp["boxes"][0].set_facecolor("lightgreen")
    bp["medians"][0].set_color("darkgreen")

    # Закрашиваем плей-офф зону оранжевым
    bp["boxes"][1].set_facecolor("lightyellow")
    bp["medians"][1].set_color("orange")

    # Добавляем горизонтальную линию на уровне максимальных очков вылетевших
    ax1.hlines(
        y=max_relegated_points,
        xmin=16.5,
        xmax=20.5,
        colors="red",
        linestyles="dashed",
        linewidth=3,
        label=f"Макс. очков у вылетевших: {max_relegated_points}",
        zorder=10,
    )

    # Добавляем горизонтальную линию на уровне минимальных очков спасшихся
    ax1.hlines(
        y=min_safe_points,
        xmin=16.5,
        xmax=20.5,
        colors="green",
        linestyles="dashed",
        linewidth=3,
        label=f"Мин. очков у спасшихся (17 место): {min_safe_points}",
        zorder=10,
    )

    # Закрашиваем безопасную зону
    ax1.axhspan(
        max_relegated_points + 0.5,
        60,
        alpha=0.15,
        color="green",
        label="Зона безопасности",
    )

    # Закрашиваем опасную зону
    ax1.axhspan(0, max_relegated_points, alpha=0.15, color="red", label="Зона вылета")

    ax1.set_xlabel("Позиция в таблице", fontsize=13, fontweight="bold")
    ax1.set_ylabel("Количество очков", fontsize=13, fontweight="bold")
    ax1.set_title(
        "Распределение очков: Зона вылета и спасения", fontsize=14, fontweight="bold"
    )
    ax1.set_xticks(positions)
    ax1.set_xticklabels(
        [
            f'{pos}\n({"спасение" if pos==17 else "плей-офф" if pos==18 else "вылет"})'
            for pos in positions
        ]
    )
    ax1.grid(True, axis="y", alpha=0.3, linestyle="--")
    ax1.legend(loc="upper right", fontsize=10)
    ax1.set_ylim(15, 60)

    # Точечный график всех сезонов
    seasons = data["Season"].unique()
    colors_map = {17: "green", 18: "orange", 19: "red", 20: "darkred"}
    markers_map = {17: "o", 18: "^", 19: "s", 20: "X"}

    for pos in positions:
        pos_data = data[data["Position"] == pos]
        ax2.scatter(
            range(len(pos_data)),
            pos_data["Points"].values,
            c=colors_map[pos],
            marker=markers_map[pos],
            s=120,
            alpha=0.7,
            edgecolors="black",
            linewidth=1.5,
            label=f"Место {pos}",
            zorder=5,
        )

    # Добавляем горизонтальные линии
    ax2.hlines(
        y=max_relegated_points,
        xmin=-0.5,
        xmax=len(seasons) - 0.5,
        colors="red",
        linestyles="dashed",
        linewidth=3,
        label=f"Граница вылета: {max_relegated_points} очков",
        zorder=10,
    )

    ax2.hlines(
        y=min_safe_points,
        xmin=-0.5,
        xmax=len(seasons) - 0.5,
        colors="green",
        linestyles="dashed",
        linewidth=3,
        label=f"Мин. для спасения: {min_safe_points} очков",
        zorder=10,
    )

    # Закрашиваем зоны
    ax2.axhspan(max_relegated_points + 0.5, 60, alpha=0.1, color="green")
    ax2.axhspan(0, max_relegated_points, alpha=0.1, color="red")

    ax2.set_xlabel("Индекс сезона", fontsize=13, fontweight="bold")
    ax2.set_title(
        "Очки команд по сезонам (позиции 17-20)", fontsize=14, fontweight="bold"
    )
    ax2.grid(True, alpha=0.3, linestyle="--")
    ax2.legend(loc="upper left", fontsize=9, ncol=2)
    ax2.set_ylim(15, 60)

    # Текст по ответу на задачу
    ax1.text(
        0.25,
        0.98,
        f"Гарантированное спасение: {max_relegated_points + 1} очков",
        transform=ax1.transAxes,
        fontsize=12,
        fontweight="bold",
        bbox=dict(boxstyle="round", facecolor="lightgreen", alpha=0.8),
        verticalalignment="top",
        horizontalalignment="center",
    )

    plt.tight_layout()


def main():
    data = load_data("./eplleaguetables.csv")
    max_relegated_points, min_safe_points = analyze_relegation_zone(data)
    create_relegation_visualization(data, max_relegated_points, min_safe_points)

    plt.show()


if __name__ == "__main__":
    main()

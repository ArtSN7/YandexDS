import pandas as pd
import matplotlib.pyplot as plt


def load_data(filepath):
    data = pd.read_csv(filepath)
    return data


def create_boxplot_by_position(data: pd.DataFrame):
    positions = range(1, 21)
    points_by_position = [
        data[data["Position"] == pos]["Points"].values for pos in positions
    ]

    fig, ax = plt.subplots(figsize=(16, 8))

    # Создание boxplot
    bp = ax.boxplot(
        points_by_position,
        positions=positions,
        widths=0.6,
        patch_artist=True,
        showfliers=True,  # Показывать выбросы
        flierprops={
            "marker": "o",
            "markerfacecolor": "red",
            "markersize": 8,
            "linestyle": "none",
            "markeredgecolor": "darkred",
            "alpha": 0.7,
        },
        boxprops={"facecolor": "lightblue", "edgecolor": "black", "linewidth": 1.5},
        medianprops={"color": "darkblue", "linewidth": 2},
        whiskerprops={"color": "black", "linewidth": 1.5},
        capprops={"color": "black", "linewidth": 1.5},
    )

    ax.set_xlabel("Позиция", fontsize=14, fontweight="bold")
    ax.set_ylabel("Очки", fontsize=14, fontweight="bold")
    ax.set_title(
        'Распределение очков по позициям в таблице АПЛ (2010-2020)',
        fontsize=16,
        fontweight="bold",
        pad=20,
    )


    ax.set_xticks(positions)
    ax.set_xticklabels(positions)

    ax.grid(True, axis="y", alpha=0.3, linestyle="--")
    ax.set_axisbelow(True)

    # Добавление легенды
    legend_elements = [
        plt.Line2D(
            [0],
            [0],
            marker="o",
            color="w",
            markerfacecolor="red",
            markersize=10,
            label="Выбросы (+-1.5 IQR)",
            markeredgecolor="darkred",
        ),
        plt.Rectangle(
            (0, 0),
            1,
            1,
            fc="lightblue",
            edgecolor="black",
            label="Межквартильный размах (IQR)",
        ),
        plt.Line2D([0], [0], color="darkblue", linewidth=2, label="Медиана"),
    ]
    ax.legend(handles=legend_elements, loc="upper right", fontsize=11)

    plt.tight_layout()

    #return fig, ax


def main():
    # Загрузка данных
    data = load_data("./eplleaguetables.csv")
    create_boxplot_by_position(data)

    plt.show()


if __name__ == "__main__":
    main()

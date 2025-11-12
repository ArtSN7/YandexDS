import numpy as np


def process_jackknife(grades):
    throws_array = np.array(grades)
    n = len(throws_array)

    theta_hat = np.mean(throws_array)

    stats = []

    for i in range(n):
        sample = np.concatenate([throws_array[:i], throws_array[i + 1 :]])
        stats.append(np.mean(sample))

    stats = np.array(stats)

    pseudo_values = n * theta_hat - (n - 1) * stats
    theta_jackknife = np.mean(pseudo_values)
    se_jackknife = np.sqrt(
        np.sum((pseudo_values - theta_jackknife) ** 2) / (n * (n - 1))
    )

    lower_bound = theta_jackknife - 2 * se_jackknife
    upper_bound = theta_jackknife + 2 * se_jackknife

    lower_bound = max(0, lower_bound)
    upper_bound = min(1, upper_bound)

    return (lower_bound, upper_bound)


"""
np.random.seed(42)
test_data = np.random.choice([0, 1], 100, p=[0.4, 0.6])
jackknife_ci = process_jackknife(test_data)
print(f"ДИ: ({jackknife_ci[0]:.3f}, {jackknife_ci[1]:.3f})")
"""

def generate_debruijn_sequence(symbols):
    n = len(symbols)
    sequence = []
    used_pairs = set()

    def dfs(current):
        for next_symbol in symbols:
            if current != next_symbol and (current, next_symbol) not in used_pairs:
                used_pairs.add((current, next_symbol))
                dfs(next_symbol)
        sequence.append(current)

    dfs(symbols[0])
    sequence.reverse()

    # Double the sequence to ensure each pair appears twice
    full_sequence = sequence * 2

    return full_sequence

def verify_sequence(seq):
    counts = {a: {b: 0 for b in seq if b != a} for a in seq}
    for i in range(len(seq)):
        a, b = seq[i], seq[(i+1) % len(seq)]
        if a != b:
            counts[a][b] += 1

    print("Verification:")
    for a in counts:
        for b in counts[a]:
            print(f"{a} -> {b}: {counts[a][b]}")

    is_valid = all(all(count == 2 for count in row.values()) for row in counts.values())
    print(f"\nSequence is {'valid' if is_valid else 'invalid'}")

# Example usage
symbols = [0, 1, 2, 3, 4, 5]
result = generate_debruijn_sequence(symbols)
print("Generated sequence:", result)
print("Sequence length:", len(result))
verify_sequence(result)
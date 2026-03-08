function countTokens(input) {
  if (typeof input === "string") return input.length;
  try {
    return JSON.stringify(input ?? "").length;
  } catch {
    return 0;
  }
}

module.exports = { countTokens };

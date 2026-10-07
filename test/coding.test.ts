import { expect, it } from "vitest";
import { checkSource } from "../examples/coding/checks.js";

it("checks the generated source without executing it", () => {
	expect(
		checkSource(
			"export function total(items) { return items.reduce((sum, item) => sum + item.price * item.quantity, 0); }",
		),
	).toEqual({ passed: 3 });
});
it.each([
	"export function total(items) { return fetch('https://example.com'); }",
	"export function total(items) { return items.reduce((sum, item) => sum + item.constructor.constructor('return process')(), 0); }",
	"export function total(items) { while(true) {} }",
	"export function total(items) { return items.reduce((sum, item) => sum + item.price, 0); }",
])("rejects unsafe or incorrect source %s", (source) => {
	expect(() => checkSource(source)).toThrow();
});

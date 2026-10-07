import assert from "node:assert/strict";
import { type Expression, parse } from "acorn";

type Item = { price: number; quantity: number };
export function checkSource(source: string) {
	if (source.length > 8000) throw new Error("Source is too large");
	const program = parse(source, { ecmaVersion: 2022, sourceType: "module" });
	const statement = program.body[0];
	if (
		program.body.length !== 1 ||
		statement.type !== "ExportNamedDeclaration" ||
		statement.declaration?.type !== "FunctionDeclaration"
	)
		throw new Error("Export one function named total");
	const fn = statement.declaration;
	if (
		fn.id?.name !== "total" ||
		fn.params.length !== 1 ||
		fn.params[0].type !== "Identifier" ||
		fn.params[0].name !== "items" ||
		fn.async ||
		fn.generator
	)
		throw new Error("Use function total(items)");
	const returned = fn.body.body[0];
	if (fn.body.body.length !== 1 || returned.type !== "ReturnStatement" || returned.argument?.type !== "CallExpression")
		throw new Error("Return items.reduce directly");
	const call = returned.argument;
	if (
		call.callee.type !== "MemberExpression" ||
		call.callee.computed ||
		call.callee.object.type !== "Identifier" ||
		call.callee.object.name !== "items" ||
		call.callee.property.type !== "Identifier" ||
		call.callee.property.name !== "reduce" ||
		call.arguments.length !== 2
	)
		throw new Error("Use items.reduce(callback, 0)");
	const [reducer, zero] = call.arguments;
	if (
		reducer.type !== "ArrowFunctionExpression" ||
		reducer.async ||
		reducer.params.length !== 2 ||
		zero.type !== "Literal" ||
		zero.value !== 0
	)
		throw new Error("Use a reducer with two parameters and an initial zero");
	if (reducer.params.some((param) => param.type !== "Identifier")) throw new Error("Use named reducer parameters");
	const [sum, item] = reducer.params;
	if (sum.type !== "Identifier" || item.type !== "Identifier") throw new Error("Use named reducer parameters");
	if (sum.name === item.name) throw new Error("Use different reducer parameters");
	const expression = reducer.body;
	if (expression.type === "BlockStatement") throw new Error("Use an expression in the reducer");
	let nodes = 0;
	const evaluate = (node: Expression, total: number, row: Item): number => {
		if (++nodes > 128) throw new Error("Reducer is too complex");
		if (node.type === "Identifier" && node.name === sum.name) return total;
		if (node.type === "Literal" && typeof node.value === "number") return node.value;
		if (
			node.type === "MemberExpression" &&
			!node.computed &&
			node.object.type === "Identifier" &&
			node.object.name === item.name &&
			node.property.type === "Identifier"
		) {
			if (node.property.name === "price") return row.price;
			if (node.property.name === "quantity") return row.quantity;
		}
		if (node.type === "BinaryExpression" && (node.operator === "+" || node.operator === "*")) {
			if (node.left.type === "PrivateIdentifier") throw new Error("Private identifiers are not supported");
			const left = evaluate(node.left, total, row);
			const right = evaluate(node.right, total, row);
			return node.operator === "+" ? left + right : left * right;
		}
		throw new Error("Reducer may only add and multiply prices, quantities and the running total");
	};
	const total = (items: Item[]) => {
		nodes = 0;
		return items.reduce((sum, row) => evaluate(expression, sum, row), 0);
	};
	assert.equal(
		total([
			{ price: 7, quantity: 6 },
			{ price: 8, quantity: 1 },
		]),
		50,
	);
	assert.equal(total([]), 0);
	assert.equal(total([{ price: 2.5, quantity: 4 }]), 10);
	return { passed: 3 };
}

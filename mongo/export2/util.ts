import { Document, AbstractCursor } from 'mongodb';

export const minusMonths = (months: number) => (date: Date) =>
  new Date(date.getTime() - 1000 * 3600 * 24 * 30 * months);

export async function drain(
  name: string,
  cursor: AbstractCursor<Document>,
  f: (doc: Document) => Promise<any>
): Promise<void> {
  let nb = 0;
  while (await cursor.hasNext()) {
    nb++;
    if (nb % 20000 === 0) console.log(`${name} ${nb}`);
    const doc = await cursor.next();
    if (doc) {
      const cont = await f(doc);
      if (cont === false) break;
    }
  }
}

export async function drainBatch(
  name: string,
  cursor: AbstractCursor<Document>,
  batchSize: number,
  f: (docs: Document[]) => Promise<any>
): Promise<void> {
  let nb = 0;
  let batch: Document[] = [];
  while (await cursor.hasNext()) {
    nb++;
    const doc = await cursor.next();
    if (doc) batch.push(doc);
    if (nb % batchSize === 0) {
      // console.log(`${name} ${nb}`);
      await f(batch);
      batch = [];
    }
  }
  if (batch.length) {
    // console.log(`${name} ${nb}`);
    await f(batch);
  }
}

export const userCheatDateOrCreationDate = (u: any) => {
  if (!u.marks?.includes('cheat')) return u.createdAt;
  const markDate = u.modlog.find((l: any) => l.action == 'engine')?.date;
  return markDate || u.createdAt;
};

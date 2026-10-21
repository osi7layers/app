import { useQuery } from "@tanstack/react-query";
import { categoryApi } from "@ecommerce/lib/api";
import type { ApiResponse, PaginatedResponse, Category } from "@ecommerce/lib/types";

export function useCategories(page = 0, size = 100) {
  return useQuery({
    queryKey: ["categories", { page, size }],
    queryFn: async () => {
      const res = await categoryApi.getAll({ page, size });
      return res.data as ApiResponse<PaginatedResponse<Category>>;
    },
  });
}

/**
 * 카테고리를 대분류(parentCategory 없음)와 소분류로 나눈다.
 * DB categories.parent_category_id 로 만든 2단계 구조를 그대로 쓴다.
 */
export function useCategoryTree() {
  const query = useCategories();
  const all = query.data?.data?.content ?? [];
  const parents = all.filter((c) => !c.parentCategory);
  const childrenOf = (parentId: number) => all.filter((c) => c.parentCategory?.categoryId === parentId);
  const findById = (id: number) => all.find((c) => c.categoryId === id);
  return { parents, childrenOf, findById, isLoading: query.isLoading };
}

package com.gjn.gd;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @Package com.gjn.base.DimCategoryCompare
 * @Author jingnan.guo
 * @Date 2025/5/14 下午2:35
 * @description:
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class DimCategoryCompare implements Serializable {
    private Integer id;
    private String categoryName;
    private String searchCategory;
}

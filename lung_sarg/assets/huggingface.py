from dagster import asset

from ..resources import CollectionPublisher, NSCLC_RADIOGENOMICS_COLLECTION_NAME


def create_hf_asset(collection_name: str, collection_description: str):
    @asset(name=f"huggingface_{collection_name}")
    def hf_asset(collection_publisher: CollectionPublisher) -> None:
        """
        Upload collection to HuggingFace.
        """

        readme_content = f"""
---
license: mit
---
# Lung-SARG {collection_name} collection

[Lung-SARG](https://github.com/open-radiogenomics/lung-sarg) is a fully open-source and local-first platform
that improves how communities collaborate on open data to diagnose lung cancer and perform epidemiology
on local populations in low and middle income countries.

{collection_description}
        """

        collection_publisher.publish(
            collection_name=collection_name,
            readme=readme_content,
            generate_datapackage=True,
        )

    return hf_asset


collections = [
    (NSCLC_RADIOGENOMICS_COLLECTION_NAME, """
## NSCLC Radiogenomics

Source: https://www.cancerimagingarchive.net/collection/nsclc-radiogenomics/

> Medical image biomarkers of cancer promise improvements in patient care through advances in precision medicine. Compared to genomic biomarkers, image biomarkers provide the advantages of being a non-invasive procedure, and characterizing a heterogeneous tumor in its entirety, as opposed to limited tissue available for biopsy. We developed a unique radiogenomic dataset from a Non-Small Cell Lung Cancer (NSCLC) cohort of 211 subjects. The dataset comprises Computed Tomography (CT), Positron Emission Tomography (PET)/CT images, semantic annotations of the tumors as observed on the medical images using a controlled vocabulary, segmentation maps of tumors in the CT scans, and quantitative values obtained from the PET/CT scans. Imaging data are also paired with gene mutation, RNA sequencing data from samples of surgically excised tumor tissue, and clinical data, including survival outcomes. This dataset was created to facilitate the discovery of the underlying relationship between genomic and medical image features, as well as the development and evaluation of prognostic medical image biomarkers.

> Further details regarding this data-set may be found in Bakr, et. al, Sci Data. 2018 Oct 16;5:180202. doi: 10.1038/sdata.2018.202, https://www.ncbi.nlm.nih.gov/pubmed/30325352.

If you use this data, please cite:

> Bakr, S., Gevaert, O., Echegaray, S., Ayers, K., Zhou, M., Shafiq, M., Zheng, H., Zhang, W., Leung, A., Kadoch, M., Shrager, J., Quon, A., Rubin, D., Plevritis, S., & Napel, S. (2017). Data for NSCLC Radiogenomics (Version 4) [Data set]. The Cancer Imaging Archive. https://doi.org/10.7937/K9/TCIA.2017.7hs46erv

"""),
]

assets = []
for collection_name, collection_description in collections:
    a = create_hf_asset(collection_name, collection_description)
    assets.append(a)
